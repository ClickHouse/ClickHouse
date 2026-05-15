#pragma once

#if defined(OS_LINUX)

#include <Interpreters/Context_fwd.h>  // ContextMutablePtr
#include <Common/EventFD.h>
#include <Common/ThreadPool.h>

#include <cstdint>
#include <optional>
#include <string>

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

    struct OOMKillCounters
    {
        /// Snapshot of cgroup v2 `memory.events:oom_kill`. This is deliberately
        /// cgroup-local; a global host counter can be advanced by unrelated
        /// processes and would make manual canary kills look like OOM events.
        std::optional<uint64_t> cgroup_oom_kill;
    };

    /// Read the OOM evidence used to classify canary death. Missing counters
    /// are represented as `std::nullopt`, which makes the response conservative:
    /// the canary may still be relaunched, but global query cancellation is not
    /// triggered without local OOM evidence.
    OOMKillCounters readOOMKillCounters() const;

    /// Returns true only when the local OOM counter moved forward between the
    /// canary spawn and its `SIGKILL`. `waitpid` alone cannot distinguish the
    /// kernel OOM killer from an operator's `kill -9`.
    bool hasOOMKillCounterAdvanced(const OOMKillCounters & before, const OOMKillCounters & after) const;

    EventFD shutdown_fd;

    std::optional<std::string> cgroup_memory_events_path;

    ThreadFromGlobalPool monitor_thread;

    ContextMutablePtr context;
    LoggerPtr log;
    const Config config;
};

}

#endif
