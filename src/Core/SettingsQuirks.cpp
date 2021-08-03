#include <Core/SettingsQuirks.h>
#include <Core/Settings.h>
#include <common/logger_useful.h>

#ifdef __linux__
#include <linux/version.h>

/// Detect does epoll_wait with nested epoll fds works correctly.
/// Polling nested epoll fds from epoll_wait is required for async_socket_for_remote and use_hedged_requests.
///
/// It may not be reliable in 5.5+ [1], that has been fixed in 5.7+ [2] or 5.6.13+.
///
///   [1]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=339ddb53d373
///   [2]: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=0c54a6a44bf3
bool nestedEpollWorks(Poco::Logger * log)
{
#if (LINUX_VERSION_CODE >= KERNEL_VERSION(5, 5, 0)) && (LINUX_VERSION_CODE < KERNEL_VERSION(5, 6, 13))
        /// the check is correct since there will be no more 5.5.x releases.
        if (log)
            LOG_WARNING(log, "Nested epoll_wait has some issues on kernels [5.5.0, 5.6.13). You should upgrade it to avoid possible issues.");
        return false;
#else
        (void)log;
        return true;
#endif
}
#else
bool nestedEpollWorks(Poco::Logger *) { return true; }
#endif

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
}

}
