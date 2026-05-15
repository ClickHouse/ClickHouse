#include <Common/OOMCanary.h>

#if defined(OS_LINUX)

#include <Common/Epoll.h>
#include <Common/OOMCanaryExitCodes.h>
#include <Common/logger_useful.h>
#include <Common/waitForPid.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/StackTrace.h>
#include <Common/FramePointers.h>
#include <Common/Jemalloc.h>
#include <base/errnoToString.h>
#include <base/cgroupsv2.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
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
#include <climits>
#include <csignal>
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
#if defined(SI_KERNEL)
constexpr Int32 OOM_CANARY_SIGNAL_CODE = SI_KERNEL;
#else
constexpr Int32 OOM_CANARY_SIGNAL_CODE = 0;
#endif

std::optional<uint64_t> readCounterFromFile(const std::string & path, std::string_view key)
{
    try
    {
        ReadBufferFromFile in(path);

        while (!in.eof())
        {
            std::string current_key;
            readStringUntilWhitespace(current_key, in);
            if (current_key.empty() && in.eof())
                break;

            assertChar(' ', in);

            uint64_t value = 0;
            readIntText(value, in);

            if (current_key == key)
                return value;

            std::string rest_of_line;
            readStringUntilNewlineInto(rest_of_line, in);
            in.tryIgnore(1);
        }
    }
    catch (...)
    {
        /// Ok: this is best-effort cgroup evidence probing. Missing or
        /// malformed files make the OOM response conservative.
        return std::nullopt;
    }

    return std::nullopt;
}

std::optional<std::string> getCgroupMemoryEventsPath()
{
    if (auto cgroup_path = getCgroupsV2PathContainingFile("memory.events"))
        return (std::filesystem::path(*cgroup_path) / "memory.events").string();
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

OOMCanary::OOMCanary(ContextMutablePtr context_, Config config_)
    : context(std::move(context_))
    , log(getLogger("OOMCanary"))
    , config(std::move(config_))
{
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

    cgroup_memory_events_path = getCgroupMemoryEventsPath();
    if (!cgroup_memory_events_path)
    {
        LOG_WARNING(log, "Cannot observe cgroup v2 `memory.events`; OOM canary will relaunch after `SIGKILL`, "
            "but will not run the OOM response without cgroup-local OOM evidence");
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
        monitor_thread.join();

    LOG_INFO(log, "OOM canary stopped");
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
        LOG_WARNING(log, "posix_spawnattr_init failed: {}", errnoToString());
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

OOMCanary::OOMKillCounters OOMCanary::readOOMKillCounters() const
{
    OOMKillCounters counters;

    /// `memory.events` is monotonic for the current cgroup and is the only
    /// signal we use for OOM response. Host-wide counters such as
    /// `/proc/vmstat:oom_kill` are intentionally ignored because another
    /// workload on the same host can advance them.
    if (cgroup_memory_events_path)
        counters.cgroup_oom_kill = readCounterFromFile(*cgroup_memory_events_path, "oom_kill");

    return counters;
}

bool OOMCanary::hasOOMKillCounterAdvanced(const OOMKillCounters & before, const OOMKillCounters & after) const
{
    /// Tests use this failpoint to exercise the recovery path without creating
    /// real memory pressure. Production builds without `libfiu` compile this to
    /// a no-op, so the decision still depends only on cgroup OOM evidence.
    bool force_oom_evidence = false;
    fiu_do_on(FailPoints::oom_canary_force_oom_evidence, { force_oom_evidence = true; });
    if (force_oom_evidence)
        return true;

    /// Prefer the current memory cgroup counter: it is local to the server's
    /// cgroup and avoids treating unrelated host OOM kills as canary events.
    if (before.cgroup_oom_kill && after.cgroup_oom_kill)
        return *after.cgroup_oom_kill > *before.cgroup_oom_kill;

    return false;
}

void OOMCanary::monitorThread()
{
    LOG_INFO(log, "OOM canary monitor thread started");

    Epoll epoll;
    epoll.add(shutdown_fd.fd, EPOLLIN);

    auto interruptible_sleep = [&](uint64_t timeout_ms) -> bool
    {
        epoll_event ev;
        const int clamped_timeout_ms = static_cast<int>(std::min<uint64_t>(timeout_ms, INT_MAX));
        return epoll.getManyReady(1, &ev, clamped_timeout_ms) > 0;
    };

    const uint64_t initial_backoff_milliseconds = config.initial_backoff_seconds * 1000;
    const uint64_t max_backoff_milliseconds = config.max_backoff_seconds * 1000;

    uint64_t backoff_milliseconds = 0;
    uint64_t attempt = 0;

    while (!interruptible_sleep(backoff_milliseconds))
    {
        ++attempt;
        if (attempt > 1 + config.max_rapid_relaunches)
        {
            LOG_WARNING(log, "OOM canary relaunch limit ({}) reached, giving up", config.max_rapid_relaunches);
            break;
        }

        const OOMKillCounters oom_kill_counters_before_canary_spawn = readOOMKillCounters();
        const pid_t pid = spawnCanary(config.size_bytes);
        if (pid <= 0)
        {
            LOG_WARNING(log, "Failed to spawn OOM canary, disabling");
            break;
        }

        const int pidfd = syscall_pidfd_open(pid);
        if (pidfd < 0)
        {
            LOG_WARNING(log, "pidfd_open failed for canary pid {}: {}; disabling", pid, errnoToString());
            reapChild(pid);
            break;
        }

        /// Wait for either the canary to die or `stop()` to signal shutdown.
        epoll.add(pidfd, EPOLLIN);
        epoll_event events[2];
        const size_t n = epoll.getManyReady(2, events, -1);

        bool canary_died = false;
        bool shutdown_requested = false;
        for (size_t i = 0; i < n; ++i)
        {
            if (events[i].data.fd == pidfd)
                canary_died = true;
            else if (events[i].data.fd == shutdown_fd.fd)
                shutdown_requested = true;
        }

        if (shutdown_requested && !canary_died)
        {
            syscall_pidfd_send_signal(pidfd, SIGKILL);
        }

        const int status = reapChild(pid);
        epoll.remove(pidfd);
        ::close(pidfd);

        if (shutdown_requested)
        {
            LOG_INFO(log, "OOM canary pid {} terminated during shutdown", pid);
            break;
        }

        bool confirmed_oom = false;
        if (WIFSIGNALED(status))
        {
            const int sig = WTERMSIG(status);
            if (sig == SIGKILL && hasOOMKillCounterAdvanced(oom_kill_counters_before_canary_spawn, readOOMKillCounters()))
                confirmed_oom = true;
            else if (sig == SIGKILL)
            {
                LOG_WARNING(log, "OOM canary pid {} killed by SIGKILL without cgroup OOM evidence; "
                    "skipping OOM response (looks like external kill)", pid);
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
        {
            onCanaryOOM();
            attempt = 0;
            backoff_milliseconds = initial_backoff_milliseconds;
        }
        else
        {
            backoff_milliseconds = std::max(initial_backoff_milliseconds,
                std::min(backoff_milliseconds * 2, max_backoff_milliseconds));
        }

        if (!config.relaunch)
            break;
    }

    LOG_INFO(log, "OOM canary monitor thread exiting");
}

void OOMCanary::onCanaryOOM()
{
    /// Response sequence: run each step in its own try/catch so a failure in
    /// one step does not skip the next.

    LOG_FATAL(log, "OOM canary killed by SIGKILL with cgroup OOM evidence. "
        "System is under severe memory pressure.");

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
