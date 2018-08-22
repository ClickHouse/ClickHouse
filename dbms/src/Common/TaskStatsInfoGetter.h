#pragma once

#include <Core/Types.h>

struct taskstats;


namespace DB
{

class Exception;


/// Get taskstat info from OS kernel via Netlink protocol.
class TaskStatsInfoGetter
{
public:
    TaskStatsInfoGetter();
    TaskStatsInfoGetter(const TaskStatsInfoGetter &) = delete;

    void getStat(::taskstats & stat, int tid);

    ~TaskStatsInfoGetter();

    /// Make a syscall and returns Linux thread id
    static int getCurrentTID();

    /// Whether the current process has permissions (sudo or cap_net_admin capabilties) to get taskstats info
    static bool checkPermissions();

private:
    void init();

    int netlink_socket_fd = -1;
    UInt16 netlink_family_id = 0;
};

}
