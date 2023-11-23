#include <Core/SettingsQuirks.h>
#include <Core/Settings.h>
#include <Poco/Environment.h>
#include <Poco/Platform.h>
#include <Common/VersionNumber.h>
#include <Common/logger_useful.h>
#include <cstdlib>

namespace
{

/// Detect does epoll_wait with nested epoll fds works correctly.
/// Polling nested epoll fds from epoll_wait is required for async_socket_for_remote and use_hedged_requests.
///
/// It may not be reliable in 5.5+ [1], that has been fixed in 5.7+ [2] or 5.6.13+.
///
///   [1]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=339ddb53d373
///   [2]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=0c54a6a44bf3
bool nestedEpollWorks(Poco::Logger * log)
{
    if (Poco::Environment::os() != POCO_OS_LINUX)
        return true;

    DB::VersionNumber linux_version(Poco::Environment::osVersion());

    /// the check is correct since there will be no more 5.5.x releases.
    if (linux_version >= DB::VersionNumber{5, 5, 0} && linux_version < DB::VersionNumber{5, 6, 13})
    {
        if (log)
            LOG_WARNING(log, "Nested epoll_wait has some issues on kernels [5.5.0, 5.6.13). You should upgrade it to avoid possible issues.");
        return false;
    }

    return true;
}

/// See also QUERY_PROFILER_DEFAULT_SAMPLE_RATE_NS in Core/Defines.h
#if !defined(SANITIZER)
bool queryProfilerWorks() { return true; }
#else
bool queryProfilerWorks() { return false; }
#endif

}

namespace DB
{

/// Update some settings defaults to avoid some known issues.
void applySettingsQuirks(Settings & settings, Poco::Logger * log)
{
    if (!nestedEpollWorks(log))
    {
        if (!settings.async_socket_for_remote.changed && settings.async_socket_for_remote)
        {
            settings.async_socket_for_remote = false;
            if (log)
                LOG_WARNING(log, "async_socket_for_remote has been disabled (you can explicitly enable it still)");
        }
        if (!settings.use_hedged_requests.changed && settings.use_hedged_requests)
        {
            settings.use_hedged_requests = false;
            if (log)
                LOG_WARNING(log, "use_hedged_requests has been disabled (you can explicitly enable it still)");
        }
    }

    if (!queryProfilerWorks())
    {
        if (settings.query_profiler_real_time_period_ns)
        {
            settings.query_profiler_real_time_period_ns = 0;
            if (log)
                LOG_WARNING(log, "query_profiler_real_time_period_ns has been disabled (due to server had been compiled with sanitizers)");
        }
        if (settings.query_profiler_cpu_time_period_ns)
        {
            settings.query_profiler_cpu_time_period_ns = 0;
            if (log)
                LOG_WARNING(log, "query_profiler_cpu_time_period_ns has been disabled (due to server had been compiled with sanitizers)");
        }
    }
}

}
