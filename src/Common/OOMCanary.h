#pragma once

#include <Interpreters/Context_fwd.h>  // ContextMutablePtr
#include <Common/ThreadPool.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

namespace DB
{

/// OOM Canary: a sacrificial child process that attracts the OOM killer
/// before the main ClickHouse server process.
///
/// The canary is a child process (created via raw `clone` syscall to avoid
/// jemalloc `pthread_atfork` deadlocks) that allocates, touches, and `mlock`-s
/// a configurable amount of memory. Its `oom_score_adj` is set to 1000
/// (the maximum), so the kernel OOM killer will target it first.
///
/// A dedicated monitor thread blocks on `waitpid`. When the canary dies from
/// `SIGKILL` and Linux OOM-kill counters confirm an OOM event, the monitor
/// executes the response sequence: purge jemalloc arenas, best-effort cancel
/// all queries, cancel all merges, queue event to `system.crash_log`, and
/// optionally relaunch. Note that synchronous system log flushing is
/// deliberately avoided under memory pressure.
///
/// Non-Linux platforms: the canary is a no-op (start returns immediately).
class OOMCanary
{
public:
    struct Config
    {
        bool enable = false;
        size_t size_bytes = 100 * 1024 * 1024; /// 100 MB
        bool relaunch = true;
    };

    explicit OOMCanary(ContextMutablePtr context_);
    ~OOMCanary();

    /// Non-copyable, non-movable
    OOMCanary(const OOMCanary &) = delete;
    OOMCanary & operator=(const OOMCanary &) = delete;
    OOMCanary(OOMCanary &&) = delete;
    OOMCanary & operator=(OOMCanary &&) = delete;

    /// Spawn the canary child process and start the monitor thread.
    /// On failure, logs a warning and returns (does not throw).
    void start(const Config & config);

    /// Stop the canary: kill the child process and join the monitor thread.
    /// Idempotent: safe to call multiple times.
    void stop();

    bool isRunning() const;

private:
#if defined(OS_LINUX)
    /// Create and set up the canary child process.
    /// Returns the child pid, or -1 on failure.
    pid_t spawnCanary(size_t size_bytes);

    /// The child process main function (runs after `clone`).
    /// Only uses async-signal-safe functions/syscalls.
    /// `page_size` and `max_fd` must be obtained before `clone`.
    [[noreturn]] static void childMain(size_t size_bytes, int64_t page_size, int max_fd, pid_t parent_pid);

    /// Monitor thread function: waitpid loop, response, optional relaunch.
    void monitorThread();

    /// Execute the OOM response sequence. This path intentionally avoids slow
    /// synchronous system-log flushing so the monitor can relaunch the canary
    /// promptly after shedding memory.
    void onCanaryDied();

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

    std::atomic<pid_t> canary_pid{-1};
    std::atomic<bool> running{false};
    std::atomic<bool> shutdown_requested{false};
    bool relaunch_enabled = false;
    size_t canary_size_bytes = 0;
    std::optional<std::string> cgroup_memory_events_path;
    OOMKillCounters oom_kill_counters_before_spawn;

    /// Protects start/stop transitions
    std::mutex state_mutex;

    ThreadFromGlobalPool monitor_thread;
#endif

    ContextMutablePtr context;
    LoggerPtr log;
};

}
