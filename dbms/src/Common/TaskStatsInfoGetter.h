#pragma once

#include <sys/types.h>
#include <Core/Types.h>
#include <boost/noncopyable.hpp>

struct taskstats;

namespace DB
{

/// Get taskstat info from OS kernel via Netlink protocol.
class TaskStatsInfoGetter : private boost::noncopyable
{
public:
    TaskStatsInfoGetter();
    ~TaskStatsInfoGetter();

    void getStat(::taskstats & stat, pid_t tid);

    /// Make a syscall and returns Linux thread id
    static pid_t getCurrentTID();

    /// Whether the current process has permissions (sudo or cap_net_admin capabilties) to get taskstats info
    static bool checkPermissions();

#if defined(__linux__)
private:
    int netlink_socket_fd = -1;
    UInt16 taskstats_family_id = 0;
#endif
};

}
