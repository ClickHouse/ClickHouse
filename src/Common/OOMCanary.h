#pragma once

#include <Interpreters/Context_fwd.h>  // ContextMutablePtr
#include <Common/ThreadPool.h>

#include <atomic>
#include <mutex>

namespace DB
{

/// OOM Canary: a sacrificial child process that attracts the OOM killer
/// before the main ClickHouse server process.
///
/// The canary is a `vfork`-ed child that allocates, touches, and mlock()-s
/// a configurable amount of memory. Its `oom_score_adj` is set to 1000
/// (the maximum), so the kernel OOM killer will target it first.
///
/// A dedicated monitor thread blocks on waitpid(). When the canary dies
/// (typically SIGKILL from OOM killer), the monitor executes the response
/// sequence: purge jemalloc arenas, kill all queries, cancel all merges,
/// write to `system.crash_log`, flush system logs, and optionally relaunch.
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

    /// The child process main function (runs after `vfork`).
    /// Only uses async-signal-safe functions/syscalls.
    /// `page_size` and `max_fd` must be obtained before `vfork`.
    [[noreturn]] static void childMain(size_t size_bytes, long page_size, int max_fd);

    /// Monitor thread function: waitpid loop, response, optional relaunch.
    void monitorThread();

    /// Execute the OOM response sequence.
    void onCanaryDied();

    std::atomic<pid_t> canary_pid{-1};
    std::atomic<bool> running{false};
    std::atomic<bool> shutdown_requested{false};
    bool relaunch_enabled = false;
    size_t canary_size_bytes = 0;

    /// Protects start/stop transitions
    std::mutex state_mutex;

    ThreadFromGlobalPool monitor_thread;
#endif

    ContextMutablePtr context;
    LoggerPtr log;
};

}
