import { type RunOptions, run } from "@grammyjs/runner";
import { computeBackoff, sleepWithAbort } from "openclaw/plugin-sdk/infra-runtime";
import { formatErrorMessage } from "openclaw/plugin-sdk/infra-runtime";
import { formatDurationPrecise } from "openclaw/plugin-sdk/infra-runtime";
import { withTelegramApiErrorLogging } from "./api-logging.js";
import { createTelegramBot } from "./bot.js";
import type { UserFromGetMe } from "./bot.runtime.js";
import { type TelegramTransport } from "./fetch.js";
import { isRecoverableTelegramNetworkError } from "./network-errors.js";

const TELEGRAM_POLL_RESTART_POLICY = {
  initialMs: 2000,
  maxMs: 30_000,
  factor: 1.8,
  jitter: 0.25,
};

/** Heartbeat supervisor constants */
const HEARTBEAT_INTERVAL_MS = 5_000;
const HEARTBEAT_TIMEOUT_MS = 5_000;
const HEARTBEAT_FAIL_THRESHOLD = 3;
const HEARTBEAT_STALE_THRESHOLD_MS = 45_000;
const UPDATES_STALE_THRESHOLD_MS = 45_000;

const POLL_STOP_GRACE_MS = 15_000;

const waitForGracefulStop = async (stop: () => Promise<void>) => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      stop(),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, POLL_STOP_GRACE_MS);
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
};

type TelegramBot = ReturnType<typeof createTelegramBot>;

type TelegramPollingSessionOpts = {
  token: string;
  config: Parameters<typeof createTelegramBot>[0]["config"];
  accountId: string;
  runtime: Parameters<typeof createTelegramBot>[0]["runtime"];
  proxyFetch: Parameters<typeof createTelegramBot>[0]["proxyFetch"];
  abortSignal?: AbortSignal;
  runnerOptions: RunOptions<unknown>;
  getLastUpdateId: () => number | null;
  persistUpdateId: (updateId: number) => Promise<void>;
  log: (line: string) => void;
  logInfo?: (line: string) => void;
  logError?: (line: string) => void;
  /** Pre-resolved Telegram transport to reuse across bot instances */
  telegramTransport?: TelegramTransport;
  /** Rebuild Telegram transport after stall/network recovery when marked dirty. */
  createTelegramTransport?: () => TelegramTransport;
};

/**
 * Managed polling instance — the "getUpdates connection" that the supervisor
 * creates on heartbeat success and destroys on network failure.
 */
type PollingInstance = {
  bot: TelegramBot;
  runner: ReturnType<typeof run>;
  fetchAbortController: AbortController;
  /** Resolves when the runner finishes (normally or via force). */
  task: Promise<void>;
  /** Signal the force-cycle path. */
  forceCycleResolve: () => void;
};

export class TelegramPollingSession {
  #restartAttempts = 0;
  #webhookCleared = false;
  #activeRunner: ReturnType<typeof run> | undefined;
  #activeFetchAbort: AbortController | undefined;
  #telegramTransport: TelegramTransport | undefined;
  #discardTransportOnRestart = false;
  /** Cached botInfo from the first successful `getMe()` call. */
  #cachedBotInfo: UserFromGetMe | undefined;

  /** Supervisor state */
  #hbSucTime = Date.now();
  #updSucTime = Date.now();
  #failCnt = 0;
  #waitingForHeartbeatRecovery = false;
  #pollingInstance: PollingInstance | undefined;

  constructor(private readonly opts: TelegramPollingSessionOpts) {
    this.#telegramTransport = opts.telegramTransport;
  }

  get activeRunner() {
    return this.#activeRunner;
  }

  markForceRestarted() {
    // Used by external unhandled-rejection handler.
    if (this.#pollingInstance) {
      this.#destroyPollingInstance("unhandled network error");
    }
  }

  markTransportDirty() {
    this.#discardTransportOnRestart = true;
  }

  abortActiveFetch() {
    this.#activeFetchAbort?.abort();
  }

  // ─── Supervisor entry point ───────────────────────────────────────────

  async runUntilAbort(): Promise<void> {
    const now0 = Date.now();
    this.#hbSucTime = now0;
    this.#updSucTime = now0;
    this.#failCnt = 0;

    while (!this.opts.abortSignal?.aborted) {
      // ── Heartbeat: probe getMe() ──
      const healthy = await this.#heartbeat();

      if (this.opts.abortSignal?.aborted) {
        break;
      }

      if (healthy) {
        if (this.#waitingForHeartbeatRecovery) {
          (this.opts.logInfo ?? this.opts.log)(
            "[telegram] Heartbeat recovered; restarting polling instance.",
          );
          this.#waitingForHeartbeatRecovery = false;
        } else if (this.#failCnt > 0) {
          (this.opts.logInfo ?? this.opts.log)(
            `[telegram] Heartbeat recovered after ${this.#failCnt} consecutive failure(s).`,
          );
        }
        this.#hbSucTime = Date.now();
        this.#failCnt = 0;

        // Ensure a polling instance is running.
        if (!this.#pollingInstance) {
          await this.#startPollingInstance();
        }
      } else {
        this.#failCnt += 1;
        if (!this.#waitingForHeartbeatRecovery) {
          (this.opts.logInfo ?? this.opts.log)(
            `[telegram] Heartbeat failed (${this.#failCnt}/${HEARTBEAT_FAIL_THRESHOLD}).`,
          );
        }
      }

      // ── Check destroy conditions ──
      const now = Date.now();
      const hbStale = now - this.#hbSucTime > HEARTBEAT_STALE_THRESHOLD_MS;
      const updStale = this.#pollingInstance && now - this.#updSucTime > UPDATES_STALE_THRESHOLD_MS;
      if (
        this.#pollingInstance &&
        (this.#failCnt >= HEARTBEAT_FAIL_THRESHOLD || hbStale || updStale)
      ) {
        const reason =
          this.#failCnt >= HEARTBEAT_FAIL_THRESHOLD
            ? `${HEARTBEAT_FAIL_THRESHOLD} consecutive heartbeat failures`
            : hbStale
              ? `heartbeat stale for ${formatDurationPrecise(now - this.#hbSucTime)}`
              : `getUpdates stale for ${formatDurationPrecise(now - this.#updSucTime)}`;
        this.#destroyPollingInstance(reason);
        this.#waitingForHeartbeatRecovery = true;
        // Mark transport dirty so next creation rebuilds it.
        this.#discardTransportOnRestart = true;
      }

      // ── Also check if polling instance died on its own ──
      if (this.#pollingInstance) {
        this.#checkPollingInstanceHealth();
      }

      // ── Wait for next heartbeat tick ──
      try {
        await sleepWithAbort(HEARTBEAT_INTERVAL_MS, this.opts.abortSignal);
      } catch {
        if (this.opts.abortSignal?.aborted) {
          break;
        }
      }
    }

    // Cleanup on exit.
    if (this.#pollingInstance) {
      this.#destroyPollingInstance("session aborted");
    }
  }

  // ─── Heartbeat ────────────────────────────────────────────────────────

  async #heartbeat(): Promise<boolean> {
    const abort = new AbortController();
    const timeout = setTimeout(() => abort.abort(), HEARTBEAT_TIMEOUT_MS);
    try {
      // Use a lightweight API call. We need a Bot instance for this,
      // but if we don't have one yet, fall back to a raw fetch.
      if (this.#pollingInstance) {
        await this.#pollingInstance.bot.api.getMe(abort.signal as never);
      } else {
        // No active bot — do a raw fetch to check connectivity.
        const url = `https://api.telegram.org/bot${this.opts.token}/getMe`;
        const fetchFn = this.opts.proxyFetch ?? globalThis.fetch;
        const res = await fetchFn(url, {
          signal: abort.signal,
          method: "GET",
        });
        if (!res.ok) {
          return false;
        }
      }
      return true;
    } catch {
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }

  // ─── Polling instance lifecycle ───────────────────────────────────────

  async #startPollingInstance(): Promise<void> {
    const bot = await this.#createPollingBot();
    if (!bot) {
      return;
    }

    const cleanupState = await this.#ensureWebhookCleanup(bot);
    if (cleanupState !== "ready") {
      return;
    }

    await this.#confirmPersistedOffset(bot);

    const fetchAbortController = this.#activeFetchAbort!;

    // ── Wire getUpdates middleware to update upd_suc_time ──
    bot.api.config.use(async (prev, method, payload, signal) => {
      if (method !== "getUpdates") {
        return prev(method, payload, signal);
      }
      try {
        const result = await prev(method, payload, signal);
        // getUpdates returned successfully → polling data plane is healthy.
        this.#updSucTime = Date.now();
        return result;
      } catch (err) {
        throw err;
      }
    });

    const runner = run(bot, this.opts.runnerOptions);
    this.#activeRunner = runner;

    // Forward session abort to fetch abort.
    const abortFetch = () => fetchAbortController?.abort();
    if (this.opts.abortSignal && fetchAbortController) {
      this.opts.abortSignal.addEventListener("abort", abortFetch, { once: true });
    }

    let forceCycleResolve!: () => void;
    const forceCyclePromise = new Promise<void>((resolve) => {
      forceCycleResolve = resolve;
    });

    // The task promise resolves when the runner finishes or is force-cycled.
    const task = Promise.race([runner.task(), forceCyclePromise])
      .catch(() => {
        // Swallow — lifecycle errors are handled by the supervisor.
      })
      .finally(() => {
        this.opts.abortSignal?.removeEventListener("abort", abortFetch);
        // If this instance is still the active one, clear it.
        if (this.#pollingInstance?.runner === runner) {
          (this.opts.logInfo ?? this.opts.log)("[telegram] Polling instance stopped on its own.");
          this.#pollingInstance = undefined;
          this.#activeRunner = undefined;
        }
      });

    this.#pollingInstance = {
      bot,
      runner,
      fetchAbortController,
      task,
      forceCycleResolve,
    };

    this.#updSucTime = Date.now();
    this.#restartAttempts = 0;
    (this.opts.logInfo ?? this.opts.log)("[telegram] Polling instance started.");
  }

  #destroyPollingInstance(reason: string): void {
    const instance = this.#pollingInstance;
    if (!instance) {
      return;
    }
    (this.opts.logError ?? this.opts.log)(`[telegram] Destroying polling instance: ${reason}.`);
    this.#pollingInstance = undefined;
    this.#activeRunner = undefined;

    // Abort all in-flight fetches.
    instance.fetchAbortController.abort();

    // Stop runner + bot gracefully, with a timeout fallback.
    const stopRunner = () => Promise.resolve(instance.runner.stop()).catch(() => {});
    const stopBot = () => Promise.resolve(instance.bot.stop()).catch(() => {});

    void waitForGracefulStop(stopRunner);
    void waitForGracefulStop(stopBot);

    // Force-cycle in case stop hangs.
    instance.forceCycleResolve();
  }

  #checkPollingInstanceHealth(): void {
    const instance = this.#pollingInstance;
    if (!instance) {
      return;
    }
    // If the runner is no longer running, the instance is dead.
    if (!instance.runner.isRunning()) {
      (this.opts.logInfo ?? this.opts.log)(
        "[telegram] Polling runner is no longer running; clearing instance.",
      );
      this.#pollingInstance = undefined;
      this.#activeRunner = undefined;
    }
  }

  // ─── Bot creation (kept from original) ────────────────────────────────

  async #createPollingBot(): Promise<TelegramBot | undefined> {
    const fetchAbortController = new AbortController();
    this.#activeFetchAbort = fetchAbortController;
    const shouldRebuildTransport = this.#discardTransportOnRestart || !this.#telegramTransport;
    const telegramTransport = shouldRebuildTransport
      ? (this.opts.createTelegramTransport?.() ?? this.#telegramTransport)
      : this.#telegramTransport;
    if (shouldRebuildTransport && telegramTransport) {
      (this.opts.logInfo ?? this.opts.log)(
        "[telegram][diag] rebuilding transport for next polling cycle",
      );
    }
    this.#telegramTransport = telegramTransport;
    this.#discardTransportOnRestart = false;
    try {
      const bot = createTelegramBot({
        token: this.opts.token,
        runtime: this.opts.runtime,
        proxyFetch: this.opts.proxyFetch,
        config: this.opts.config,
        accountId: this.opts.accountId,
        fetchAbortSignal: fetchAbortController.signal,
        updateOffset: {
          lastUpdateId: this.opts.getLastUpdateId(),
          onUpdateId: this.opts.persistUpdateId,
        },
        telegramTransport,
        botInfo: this.#cachedBotInfo,
      });
      // On the first cycle, eagerly init the bot so we can cache botInfo.
      if (!this.#cachedBotInfo) {
        const EAGER_INIT_TIMEOUT_MS = 15_000;
        const initAbort = new AbortController();
        const onSessionAbort = () => initAbort.abort();
        if (this.opts.abortSignal?.aborted) {
          initAbort.abort();
        } else {
          this.opts.abortSignal?.addEventListener("abort", onSessionAbort, { once: true });
        }
        const initTimeout = setTimeout(() => initAbort.abort(), EAGER_INIT_TIMEOUT_MS);
        try {
          // @ts-ignore — grammy AbortSignal vs native Node.js AbortSignal
          await bot.init(initAbort.signal);
          this.#cachedBotInfo = bot.botInfo;
          (this.opts.logInfo ?? this.opts.log)(
            "[telegram] Cached botInfo from initial getMe(); subsequent cycles will skip init.",
          );
        } catch {
          if (this.opts.abortSignal?.aborted) {
            return undefined;
          }
          // Non-fatal: network unavailable or timeout fired.
        } finally {
          clearTimeout(initTimeout);
          this.opts.abortSignal?.removeEventListener("abort", onSessionAbort);
        }
      }
      return bot;
    } catch (err) {
      await this.#waitBeforeRetryOnRecoverableSetupError(err, "Telegram setup network error");
      if (this.#activeFetchAbort === fetchAbortController) {
        this.#activeFetchAbort = undefined;
      }
      return undefined;
    }
  }

  async #ensureWebhookCleanup(bot: TelegramBot): Promise<"ready" | "retry" | "exit"> {
    if (this.#webhookCleared) {
      return "ready";
    }
    try {
      await withTelegramApiErrorLogging({
        operation: "deleteWebhook",
        runtime: this.opts.runtime,
        fn: () => bot.api.deleteWebhook({ drop_pending_updates: false }),
      });
      this.#webhookCleared = true;
      return "ready";
    } catch (err) {
      const shouldRetry = await this.#waitBeforeRetryOnRecoverableSetupError(
        err,
        "Telegram webhook cleanup failed",
      );
      return shouldRetry ? "retry" : "exit";
    }
  }

  async #confirmPersistedOffset(bot: TelegramBot): Promise<void> {
    const lastUpdateId = this.opts.getLastUpdateId();
    if (lastUpdateId === null || lastUpdateId >= Number.MAX_SAFE_INTEGER) {
      return;
    }
    try {
      await bot.api.getUpdates({ offset: lastUpdateId + 1, limit: 1, timeout: 0 });
    } catch {
      // Non-fatal.
    }
  }

  // ─── Helpers ──────────────────────────────────────────────────────────

  async #waitBeforeRestart(buildLine: (delay: string) => string): Promise<boolean> {
    this.#restartAttempts += 1;
    const delayMs = computeBackoff(TELEGRAM_POLL_RESTART_POLICY, this.#restartAttempts);
    const delay = formatDurationPrecise(delayMs);
    this.opts.log(buildLine(delay));
    try {
      await sleepWithAbort(delayMs, this.opts.abortSignal);
    } catch (sleepErr) {
      if (this.opts.abortSignal?.aborted) {
        return false;
      }
      throw sleepErr;
    }
    return true;
  }

  async #waitBeforeRetryOnRecoverableSetupError(err: unknown, logPrefix: string): Promise<boolean> {
    if (this.opts.abortSignal?.aborted) {
      return false;
    }
    if (!isRecoverableTelegramNetworkError(err, { context: "unknown" })) {
      throw err;
    }
    return this.#waitBeforeRestart(
      (delay) => `${logPrefix}: ${formatErrorMessage(err)}; retrying in ${delay}.`,
    );
  }
}
