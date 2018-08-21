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

#if defined(__linux__)
    void getStat(::taskstats & stat, int tid = -1);
    bool tryGetStat(::taskstats & stat, int tid = -1);
#endif

    ~TaskStatsInfoGetter();

    /// Make a syscall and returns Linux thread id
    static int getCurrentTID();

    /// Whether the current process has permissions (sudo or cap_net_admin capabilties) to get taskstats info
    static bool checkProcessHasRequiredPermissions();

private:
    /// Caches current thread tid to avoid extra sys calls
    int getDefaultTID();
    int default_tid = -1;

#if defined(__linux__)
    bool getStatImpl(int tid, ::taskstats & out_stats, bool throw_on_error = false);
#endif

    void init();

    int netlink_socket_fd = -1;
    UInt16 netlink_family_id = 0;
};

}
