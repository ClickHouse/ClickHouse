#include <Common/OOMCanary/OOMCanary.h>

#if defined(OS_LINUX)

#include <Common/Epoll.h>
#include <Common/OOMCanaryExitCodes.h>
#include <Common/logger_useful.h>
#include <Common/waitForPid.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/StackTrace.h>
#include <Common/FramePointers.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <base/errnoToString.h>
#include <base/cgroupsv2.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/CrashLog.h>
#include <Storages/MergeTree/MergeList.h>

#include <spawn.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <fstream>
#include <limits>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

extern char ** environ;

namespace DB
{

namespace
{
constexpr Int32 OOM_CANARY_SIGNAL_CODE = SI_KERNEL;

std::optional<std::string> getCgroupMemoryEventsPath()
{
    if (auto cgroup_path = getCgroupsV2PathContainingFile("memory.events"))
        return (std::filesystem::path(*cgroup_path) / "memory.events").string();

    LOG_WARNING(getLogger("OOMCanary"),
        "Cannot observe cgroup v2 `memory.events`; OOM canary will relaunch after `SIGKILL`, "
        "but will not run the OOM response without cgroup-local OOM evidence");
    return std::nullopt;
}

/// Block until `pid` is reaped, retrying on EINTR. Returns the exit status.
int reapChild(pid_t pid)
{
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    return status;
}
}

namespace FailPoints
{
extern const char oom_canary_force_oom_evidence[];
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

OOMCanary::OOMCanary(ContextMutablePtr context_, Config config_)
    : context(std::move(context_))
    , log(getLogger("OOMCanary"))
    , config(std::move(config_))
{
    static constexpr uint64_t max_allowed_backoff_seconds = std::numeric_limits<int>::max() / 1000;
    if (config.max_backoff_seconds > max_allowed_backoff_seconds)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "oom_canary_max_backoff_seconds = {} is too large; maximum supported is {}",
            config.max_backoff_seconds, max_allowed_backoff_seconds);

    if (config.initial_backoff_seconds > config.max_backoff_seconds)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "oom_canary_initial_backoff_seconds = {} must not exceed oom_canary_max_backoff_seconds = {}",
            config.initial_backoff_seconds, config.max_backoff_seconds);
}

OOMCanary::~OOMCanary()
{
    stop();
}

void OOMCanary::start()
{
    if (config.size_bytes == 0)
    {
        LOG_WARNING(log, "OOM canary size is 0 bytes, continuing without canary");
        return;
    }

    try
    {
        monitor_thread = ThreadFromGlobalPool([this] { monitorThread(); });
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to start OOM canary monitor thread: {}", getCurrentExceptionMessage(true));
    }
}

void OOMCanary::stop()
{
    shutdown_fd.write();

    if (monitor_thread.joinable())
    {
        monitor_thread.join();
        LOG_INFO(log, "OOM canary stopped");
    }
}

pid_t OOMCanary::spawnCanary(size_t size_bytes)
{
    const pid_t parent_pid = ::getpid();

    String size_str = toString(size_bytes);
    String ppid_str = toString(parent_pid);

    char prog[] = "clickhouse";
    char mode[] = "oom-canary";
    char * argv[] = {prog, mode, size_str.data(), ppid_str.data(), nullptr};

    posix_spawnattr_t attr;
    if (::posix_spawnattr_init(&attr) != 0)
    {
        return -1;
    }
    ::posix_spawnattr_setflags(&attr, POSIX_SPAWN_USEVFORK);

    pid_t pid = -1;
    int rc = ::posix_spawn(&pid, "/proc/self/exe", nullptr, &attr, argv, environ);
    ::posix_spawnattr_destroy(&attr);

    if (rc != 0)
    {
        return -1;
    }

    LOG_INFO(log, "OOM canary child process spawned with pid {}, memory size {} bytes", pid, size_bytes);
    return pid;
}

std::optional<OOMCanary::OOMKillCounter> OOMCanary::readOOMKillCounter()
{
    /// `memory.events` is monotonic for the current cgroup and is the only
    /// signal we use for OOM response. Host-wide counters such as
    /// `/proc/vmstat:oom_kill` are intentionally ignored because another
    /// workload on the same host can advance them.
    static const std::optional<std::string> path = getCgroupMemoryEventsPath();
    if (!path)
        return std::nullopt;

    std::ifstream f(*path);
    if (!f.is_open())
        return std::nullopt;

    std::string line;
    while (std::getline(f, line))
    {
        static constexpr std::string_view prefix = "oom_kill ";
        if (!line.starts_with(prefix))
            continue;
        uint64_t value = 0;
        const char * begin = line.data() + prefix.size();
        const char * end = line.data() + line.size();
        if (std::from_chars(begin, end, value).ec == std::errc{})
            return OOMKillCounter{value};
        return std::nullopt;
    }
    return std::nullopt;
}

bool OOMCanary::oomKilled(std::optional<OOMKillCounter> before, std::optional<OOMKillCounter> after)
{
    return before && after && *after > *before;
}

void OOMCanary::monitorThread()
try
{
    LOG_INFO(log, "OOM canary monitor thread started");

    Epoll epoll;
    epoll.add(shutdown_fd.fd, EPOLLIN);

    const uint64_t initial_backoff_milliseconds = config.initial_backoff_seconds * 1000;
    const uint64_t max_backoff_milliseconds = config.max_backoff_seconds * 1000;

    uint64_t backoff_milliseconds = 0;
    uint64_t attempt = 0;

    while (true)
    {
        const std::optional<OOMKillCounter> oom_kill_counter_before_canary_spawn = readOOMKillCounter();
        const pid_t pid = spawnCanary(config.size_bytes);
        if (pid <= 0)
        {
            LOG_WARNING(log, "Failed to spawn OOM canary, disabling");
            break;
        }

        const int pidfd = syscall_pidfd_open(pid);
        if (pidfd < 0)
        {
            LOG_WARNING(log, "pidfd_open failed for canary pid {}: {}; the OOM canary requires Linux >= 5.3, disabling",
                pid, errnoToString());
            ::kill(pid, SIGKILL);
            reapChild(pid);
            break;
        }

        /// Measure how long this canary stays alive. A canary that survives
        /// well past the maximum backoff window was not part of a rapid
        /// relaunch burst, so its eventual death must not count toward the
        /// thrash guard (see the reset in the throttling section below).
        Stopwatch canary_lifetime;

        /// From here the canary child and its `pidfd` are live and must be
        /// reaped and closed on every path. `Epoll::add` / `getManyReady` /
        /// `remove` can throw (e.g. `ENOMEM` under memory pressure); without
        /// cleanup the child would be left alive and unmonitored and the
        /// `pidfd` would leak in the server process, breaking the canary
        /// ownership invariant. Wrap the whole per-child section so a throw
        /// best-effort kills, reaps, and closes before disabling the canary.
        int status = 0;
        bool reaped = false;
        bool shutdown_requested = false;
        try
        {
            /// Wait for either the canary to die or `stop()` to signal shutdown.
            epoll.add(pidfd, EPOLLIN);
            epoll_event events[2];
            events[0].data.fd = events[1].data.fd = -1;
            const size_t n = epoll.getManyReady(2, events, -1);

            bool canary_died = false;
            for (size_t i = 0; i < n; ++i)
            {
                if (events[i].data.fd == pidfd)
                    canary_died = true;
                else if (events[i].data.fd == shutdown_fd.fd)
                    shutdown_requested = true;
            }

            if (shutdown_requested && !canary_died)
            {
                if (syscall_pidfd_send_signal(pidfd, SIGKILL) != 0)
                    ::kill(pid, SIGKILL);
            }

            status = reapChild(pid);
            reaped = true;
            epoll.remove(pidfd);
        }
        catch (...)
        {
            LOG_WARNING(log, "Error while monitoring OOM canary pid {}: {}; killing it and disabling the canary",
                pid, getCurrentExceptionMessage(true));
            if (!reaped)
            {
                ::kill(pid, SIGKILL);
                reapChild(pid);
            }
            (void)::close(pidfd);
            break;
        }

        if (::close(pidfd) != 0)
            LOG_WARNING(log, "close(pidfd) failed for canary pid {}: {}", pid, errnoToString());

        if (shutdown_requested)
        {
            LOG_INFO(log, "OOM canary pid {} terminated during shutdown", pid);
            break;
        }

        bool confirmed_oom = false;
        if (WIFSIGNALED(status))
        {
            const int sig = WTERMSIG(status);
            if (sig == SIGKILL)
            {
                confirmed_oom = oomKilled(oom_kill_counter_before_canary_spawn, readOOMKillCounter());
                fiu_do_on(FailPoints::oom_canary_force_oom_evidence, { confirmed_oom = true; });
            }
            else
            {
                LOG_WARNING(log, "OOM canary pid {} killed by signal {}; skipping OOM response "
                    "(only SIGKILL is treated as an OOM trigger)", pid, sig);
            }
        }
        else if (WIFEXITED(status))
        {
            const int code = WEXITSTATUS(status);
            if (code == OOMCanaryExitCodes::TRANSIENT)
            {
                LOG_WARNING(log, "OOM canary pid {} exited with code {} (transient resource failure)", pid, code);
            }
            else
            {
                LOG_WARNING(log, "OOM canary pid {} exited with code {}; permanent setup failure, disabling", pid, code);
                break;
            }
        }

        if (confirmed_oom)
            onCanaryOOM();

        /// `oom_canary_max_rapid_relaunches` and the doubling backoff throttle
        /// *rapid* relaunches under sustained memory pressure. If instead the
        /// canary ran stably for longer than the maximum backoff window before
        /// dying, the earlier deaths were unrelated sporadic events, not a
        /// thrash burst — reset the throttle so a long-lived canary that dies
        /// only occasionally over the server's lifetime is never permanently
        /// disabled and does not carry a stale maxed-out backoff.
        if (canary_lifetime.elapsedMilliseconds() > max_backoff_milliseconds)
        {
            attempt = 0;
            backoff_milliseconds = 0;
        }

        /// Backoff and attempt accumulate across consecutive rapid relaunches.
        backoff_milliseconds = std::clamp(backoff_milliseconds * 2,
            initial_backoff_milliseconds, max_backoff_milliseconds);

        if (!config.relaunch)
            break;

        ++attempt;
        if (attempt > config.max_rapid_relaunches)
        {
            LOG_WARNING(log, "OOM canary relaunch limit ({}) reached, giving up", config.max_rapid_relaunches);
            break;
        }

        /// Sleep for `backoff_milliseconds`, break early if `stop()` signals shutdown.
        epoll_event ev{};
        if (epoll.getManyReady(1, &ev, static_cast<int>(backoff_milliseconds)) > 0)
            break;
    }

    LOG_INFO(log, "OOM canary monitor thread exiting");
}
catch (...)
{
    /// Setup before the per-child loop (e.g. `Epoll` construction or adding
    /// `shutdown_fd`) can throw with `EMFILE` / `ENOMEM`. Catch here so the
    /// exception is not recorded by `ThreadFromGlobalPool` and later surfaced
    /// as an unrelated scheduling failure; the canary just disables cleanly.
    tryLogCurrentException(log, "OOM canary monitor thread failed, disabling the canary");
}

void OOMCanary::onCanaryOOM()
{
    /// Response sequence: run each step in its own try/catch so a failure in
    /// one step does not skip the next.

    LOG_FATAL(log, "OOM canary killed by SIGKILL with cgroup OOM evidence. "
        "System is under severe memory pressure. Total tracked memory: {}, RSS: {}",
        ReadableSize(total_memory_tracker.get()), ReadableSize(total_memory_tracker.getRSS()));

    /// Purge jemalloc arenas
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

    /// Kill all queries (uses existing CancelReason::CANCELLED_BY_USER)
    try
    {
        context->getProcessList().killAllQueries();
        LOG_INFO(log, "Killed all running queries");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to kill all queries: {}", getCurrentExceptionMessage(true));
    }

    /// Cancel all merges and mutations
    try
    {
        context->getMergeList().cancelAll();
        LOG_INFO(log, "Cancelled all running merges");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to cancel all merges: {}", getCurrentExceptionMessage(true));
    }

    /// Write to system.crash_log
    try
    {
        StackTrace stack_trace(NoCapture{});
        FramePointers current_exception_trace{};
        collectCrashLog(
            /*signal=*/ 9,
            /*signal_code=*/ OOM_CANARY_SIGNAL_CODE,
            /*thread_id=*/ CurrentThread::get().thread_id,
            /*query_id=*/ "OOMCanary",
            /*query=*/ "",
            /*stack_trace=*/ stack_trace,
            /*fault_address=*/ std::nullopt,
            /*fault_access_type=*/ "",
            /*signal_description=*/ "OOM Canary: canary process killed by SIGKILL (cgroup OOM evidence detected)",
            /*current_exception_trace=*/ current_exception_trace,
            /*current_exception_trace_size=*/ 0);
        LOG_INFO(log, "Queued OOM canary event in system.crash_log");
    }
    catch (...)
    {
        LOG_WARNING(log, "Failed to write to system.crash_log: {}", getCurrentExceptionMessage(true));
    }
}

}

#endif
