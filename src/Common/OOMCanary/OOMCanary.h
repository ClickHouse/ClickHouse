#pragma once

#if defined(OS_LINUX)

#include <Interpreters/Context_fwd.h>  // ContextMutablePtr
#include <Common/EventFD.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <base/strong_typedef.h>

#include <cstdint>
#include <optional>

namespace DB
{

/// OOM Canary: a sacrificial child process that attracts the OOM killer
/// before the main ClickHouse server process.
///
/// The canary is a separate `clickhouse oom-canary` sub-process spawned via
/// posix_spawn, so it inherits nothing from the parent's
/// address space — no COW residue under memory pressure, no jemalloc state,
/// no signal handlers. The child allocates, touches, and `mlock`-s a
/// configurable amount of memory. Its `oom_score_adj` is set to 1000
/// (the maximum), so the kernel OOM killer will target it first.
///
/// A dedicated monitor thread owns the canary's pidfd and `epoll`-waits on
/// either the canary's death or a shutdown event. When the canary dies from
/// `SIGKILL` and Linux OOM-kill counters confirm an OOM event, the monitor
/// executes the response sequence: purge jemalloc arenas, best-effort cancel
/// all queries, cancel all merges, queue event to `system.crash_log`, and
/// optionally relaunch. Note that synchronous system log flushing is
/// deliberately avoided under memory pressure.
///
/// Requires Linux >= 5.3: the monitor owns the canary via `pidfd_open` and
/// `epoll`-waits on the pidfd, so there is no `/proc`-polling fallback. On
/// older kernels `pidfd_open` fails and the canary disables itself at startup.
class OOMCanary
{
public:
    struct Config
    {
        size_t size_bytes = 100 * 1024 * 1024; /// 100 MB
        bool relaunch = true;
        /// Relaunch backoff policy (applies only when `relaunch` is true).
        uint64_t max_rapid_relaunches = 10;
        uint64_t initial_backoff_seconds = 1;
        uint64_t max_backoff_seconds = 60;
    };

    OOMCanary(ContextMutablePtr context_, Config config_);
    ~OOMCanary();

    /// Non-copyable, non-movable
    OOMCanary(const OOMCanary &) = delete;
    OOMCanary & operator=(const OOMCanary &) = delete;
    OOMCanary(OOMCanary &&) = delete;
    OOMCanary & operator=(OOMCanary &&) = delete;

    /// Launch the monitor thread.
    void start();

    /// Stop the canary: signal shutdown and join the monitor thread.
    /// Idempotent: safe to call multiple times.
    void stop();

private:
    /// Spawn the canary process via posix_spawn.
    /// Returns the child pid, or -1 on failure.
    pid_t spawnCanary(size_t size_bytes);

    /// Monitor thread function: spawn, wait via epoll on (pidfd, shutdown_fd),
    /// classify the death, optionally relaunch.
    void monitorThread();

    /// Execute the OOM response sequence.
    void onCanaryOOM();

    using OOMKillCounter = StrongTypedef<uint64_t, struct OOMKillCounterTag>;

    /// Read the cgroup v2 `memory.events:oom_kill` counter. This is deliberately
    /// cgroup-local; a global host counter can be advanced by unrelated processes
    /// and would make manual canary kills look like OOM events. Returns nullopt
    /// when the path is unset or the read fails.
    static std::optional<OOMKillCounter> readOOMKillCounter();

    static bool oomKilled(std::optional<OOMKillCounter> before, std::optional<OOMKillCounter> after);

    EventFD shutdown_fd;

    ThreadFromGlobalPool monitor_thread;

    ContextMutablePtr context;
    LoggerPtr log;
    const Config config;
};

}

#endif
