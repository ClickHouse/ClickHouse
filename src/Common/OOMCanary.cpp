#include <Common/OOMCanary.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/FramePointers.h>
#include <base/errnoToString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/CrashLog.h>
#include <Storages/MergeTree/MergeList.h>

#if defined(OS_LINUX)
#    include <dlfcn.h>
#    include <Common/Jemalloc.h>
#    include <sys/mman.h>
#    include <sys/types.h>
#    include <sys/wait.h>
#    include <signal.h>
#    include <unistd.h>
#    include <fcntl.h>
#    include <sys/syscall.h>
#    include <cerrno>
#    include <cstring>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_FORK;
}

OOMCanary::OOMCanary(ContextMutablePtr context_)
    : context(std::move(context_))
    , log(getLogger("OOMCanary"))
{
}

OOMCanary::~OOMCanary()
{
    stop();
}

#if defined(OS_LINUX)

void OOMCanary::start(const Config & config)
{
    if (!config.enable)
    {
        LOG_INFO(log, "OOM canary is disabled");
        return;
    }

    if (config.size_bytes == 0)
    {
        LOG_WARNING(log, "OOM canary size is 0 bytes, continuing without canary");
        return;
    }

    std::lock_guard lock(state_mutex);

    if (running.load(std::memory_order_relaxed))
    {
        LOG_WARNING(log, "OOM canary is already running");
        return;
    }

    relaunch_enabled = config.relaunch;
    canary_size_bytes = config.size_bytes;
    shutdown_requested.store(false, std::memory_order_relaxed);

    pid_t pid = spawnCanary(config.size_bytes);
    if (pid < 0)
    {
        LOG_WARNING(log, "Failed to spawn OOM canary child process, continuing without canary");
        return;
    }

    canary_pid.store(pid, std::memory_order_relaxed);
    running.store(true, std::memory_order_release);

    try
    {
        monitor_thread = ThreadFromGlobalPool([this] { monitorThread(); });
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to start OOM canary monitor thread: {}", getCurrentExceptionMessage(true));
        /// Kill the child we just spawned since we cannot monitor it
        pid_t cpid = canary_pid.load(std::memory_order_relaxed);
        ::kill(cpid, SIGKILL);
        int status = 0;
        while (::waitpid(cpid, &status, 0) < 0 && errno == EINTR)
            ;
        canary_pid.store(-1, std::memory_order_relaxed);
        running.store(false, std::memory_order_relaxed);
    }
}

void OOMCanary::stop()
{
    std::lock_guard lock(state_mutex);

    /// Signal shutdown even if running is already false — the monitor thread
    /// may have exited on its own (e.g. relaunch failure) but still needs joining.
    shutdown_requested.store(true, std::memory_order_release);

    /// Kill the canary child to unblock waitpid in the monitor thread
    {
        pid_t cpid = canary_pid.load(std::memory_order_relaxed);
        if (cpid > 0)
            ::kill(cpid, SIGKILL);
    }

    if (monitor_thread.joinable())
        monitor_thread.join();

    canary_pid.store(-1, std::memory_order_relaxed);
    running.store(false, std::memory_order_relaxed);

    LOG_INFO(log, "OOM canary stopped");
}

bool OOMCanary::isRunning() const
{
    return running.load(std::memory_order_acquire);
}

pid_t OOMCanary::spawnCanary(size_t size_bytes)
{
    /// Get child parameters before `vfork`, because the child must stay in the
    /// async-signal-safe subset until it either `exec`s or calls `_exit`.
    long page_size = ::sysconf(_SC_PAGESIZE);
    if (page_size <= 0)
        page_size = 4096;

    int max_fd = static_cast<int>(::sysconf(_SC_OPEN_MAX));
    if (max_fd < 0 || max_fd > 65536)
        max_fd = 65536;

#if !defined(USE_MUSL)
    /// Resolve `vfork` ahead of time to avoid lazy symbol lookup in the child.
    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");
#else
    static void * real_vfork = reinterpret_cast<void *>(&vfork);
#endif

    if (!real_vfork)
    {
        LOG_WARNING(log, "Cannot resolve `vfork` for OOM canary");
        return -1;
    }

    pid_t pid = reinterpret_cast<pid_t(*)()>(real_vfork)();

    if (pid < 0)
    {
        LOG_WARNING(log, "vfork() failed for OOM canary: {}", errnoToString());
        return -1;
    }

    if (pid == 0)
    {
        /// Child process: only async-signal-safe operations from here
        childMain(size_bytes, page_size, max_fd);
        /// childMain is [[noreturn]]
    }

    LOG_INFO(log, "OOM canary child process spawned with pid {}, memory size {} bytes", pid, size_bytes);
    return pid;
}

[[noreturn]] void OOMCanary::childMain(size_t size_bytes, long page_size, int max_fd)
{
    /// Close all inherited file descriptors (3..max_fd)
    /// Prefer close_range syscall (Linux 5.9+) for efficiency,
    /// as the inherited fd limit can be very large.
    {
#if defined(__NR_close_range)
        /// close_range(3, ~0U, 0) closes all fds from 3 to max in one syscall
        if (::syscall(__NR_close_range, 3U, ~0U, 0) == 0)
        {
            /// Success — all fds closed efficiently
        }
        else
#endif
        {
            /// Fallback: iterate through all possible fds
            for (int fd = 3; fd < max_fd; ++fd)
                ::close(fd);
        }
    }

    /// Write oom_score_adj = 1000 (maximum, OOM killer targets this first)
    {
        int fd = ::open("/proc/self/oom_score_adj", O_WRONLY);
        if (fd >= 0)
        {
            const char * score = "1000";
            ssize_t written = ::write(fd, score, 4);
            (void)written; /// Best effort; warning is in parent process log
            ::close(fd);
        }
        /// D-05: If write fails, continue running the canary
    }

    /// Allocate memory with mmap (async-signal-safe on Linux)
    void * mem = ::mmap(nullptr, size_bytes, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (mem == MAP_FAILED)
        ::_exit(1);

    /// Touch every page to ensure physical memory is allocated
    char * ptr = static_cast<char *>(mem);
    for (size_t offset = 0; offset < size_bytes; offset += static_cast<size_t>(page_size))
        ptr[offset] = static_cast<char>(offset & 0xFF);

    /// Try to mlock the memory so it cannot be swapped out
    /// D-04: If mlock fails, degrade to unlocked memory and continue
    ::mlock(mem, size_bytes);

    /// Wait for a signal (the parent will SIGKILL us to stop, or OOM killer will SIGKILL us)
    sigset_t mask;
    ::sigfillset(&mask);
    ::sigprocmask(SIG_SETMASK, &mask, nullptr);

    /// Block all signals and wait — sigsuspend atomically unblocks and waits
    sigset_t empty_mask;
    ::sigemptyset(&empty_mask);

    for (;;)
        ::sigsuspend(&empty_mask);
}

void OOMCanary::monitorThread()
{
    LOG_INFO(log, "OOM canary monitor thread started, watching pid {}", canary_pid.load(std::memory_order_relaxed));

    while (!shutdown_requested.load(std::memory_order_acquire))
    {
        int status = 0;
        pid_t current_pid = canary_pid.load(std::memory_order_relaxed);
        pid_t result = ::waitpid(current_pid, &status, 0);

        if (result < 0)
        {
            if (errno == EINTR)
                continue;

            LOG_ERROR(log, "waitpid() failed for OOM canary pid {}: {}", current_pid, errnoToString());
            break;
        }

        if (result == current_pid)
        {
            bool should_run_oom_response = false;

            /// Check if this is a shutdown-initiated kill
            if (shutdown_requested.load(std::memory_order_acquire))
            {
                LOG_INFO(log, "OOM canary child pid {} terminated during shutdown", current_pid);
                break;
            }

            /// The canary was killed (likely by OOM killer)
            if (WIFSIGNALED(status))
            {
                int sig = WTERMSIG(status);

                if (sig == SIGKILL)
                {
                    should_run_oom_response = true;

                    LOG_FATAL(log, "OOM canary child pid {} was killed by signal {}. "
                        "This likely indicates the system is under severe memory pressure.",
                        current_pid, sig);
                }
                else
                {
                    LOG_WARNING(log, "OOM canary child pid {} was killed by signal {}. "
                        "Skipping OOM response because only `SIGKILL` is treated as an OOM canary trigger.",
                        current_pid, sig);
                }
            }
            else if (WIFEXITED(status))
            {
                LOG_WARNING(log, "OOM canary child pid {} exited with code {}. "
                    "Skipping OOM response because the canary was not killed by `SIGKILL`.",
                    current_pid, WEXITSTATUS(status));
            }

            canary_pid.store(-1, std::memory_order_relaxed);

            if (should_run_oom_response)
                onCanaryDied();

            /// Optionally relaunch the canary
            if (relaunch_enabled && !shutdown_requested.load(std::memory_order_acquire))
            {
                LOG_INFO(log, "Attempting to relaunch OOM canary");
                pid_t new_pid = spawnCanary(canary_size_bytes);
                if (new_pid > 0)
                {
                    canary_pid.store(new_pid, std::memory_order_relaxed);
                    LOG_INFO(log, "OOM canary relaunched with new pid {}", new_pid);
                    continue;
                }
                else
                {
                    LOG_WARNING(log, "Failed to relaunch OOM canary (memory pressure too high for fork), giving up");
                    break;
                }
            }
            else
            {
                break;
            }
        }
    }

    running.store(false, std::memory_order_release);
    LOG_INFO(log, "OOM canary monitor thread exiting");
}

void OOMCanary::onCanaryDied()
{
    /// D-06: Response sequence, each step in its own try/catch

    /// Step 1: LOG_FATAL (already done in monitorThread before calling this)

    /// Step 2: Purge jemalloc arenas
    try
    {
#if USE_JEMALLOC
        Jemalloc::purgeArenas();
        LOG_INFO(log, "Purged jemalloc arenas");
#endif
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to purge jemalloc arenas: {}", getCurrentExceptionMessage(true));
    }

    /// Step 3: Kill all queries (D-08: uses existing CancelReason::CANCELLED_BY_USER)
    try
    {
        context->getProcessList().killAllQueries();
        LOG_INFO(log, "Killed all running queries");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to kill all queries: {}", getCurrentExceptionMessage(true));
    }

    /// Step 4: Cancel all merges (D-12)
    try
    {
        context->getMergeList().cancelAll();
        LOG_INFO(log, "Cancelled all running merges");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to cancel all merges: {}", getCurrentExceptionMessage(true));
    }

    /// Step 5: Write to system.crash_log
    try
    {
        StackTrace empty_trace(NoCapture{});
        FramePointers empty_frames{};
        collectCrashLog(
            /*signal=*/9,
            /*signal_code=*/0,
            /*thread_id=*/0,
            /*query_id=*/"",
            /*query=*/"",
            /*stack_trace=*/empty_trace,
            /*fault_address=*/std::nullopt,
            /*fault_access_type=*/"",
            /*signal_description=*/"OOM Canary: canary process killed by SIGKILL",
            /*current_exception_trace=*/empty_frames,
            /*current_exception_trace_size=*/0);
        LOG_INFO(log, "Wrote OOM canary event to system.crash_log");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to write to system.crash_log: {}", getCurrentExceptionMessage(true));
    }

    /// Step 6: Flush system logs
    try
    {
        context->handleCrash();
        LOG_INFO(log, "Flushed system logs");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to flush system logs: {}", getCurrentExceptionMessage(true));
    }
}

#else // !OS_LINUX

void OOMCanary::start(const Config & config)
{
    if (config.enable)
        LOG_WARNING(log, "OOM canary is only supported on Linux, ignoring");
}

void OOMCanary::stop()
{
}

bool OOMCanary::isRunning() const
{
    return false;
}

#endif // OS_LINUX

}
