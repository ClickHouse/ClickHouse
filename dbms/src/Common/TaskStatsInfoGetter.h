#pragma once
#include <Core/Types.h>

struct taskstats;


namespace DB
{

/// Get taskstat infor from OS kernel via Netlink protocol
class TaskStatsInfoGetter
{
public:

    TaskStatsInfoGetter();
    TaskStatsInfoGetter(const TaskStatsInfoGetter &) = delete;

    void getStat(::taskstats & stat, int tid = -1) const;
    bool tryGetStat(::taskstats & stat, int tid = -1) const;

    /// Returns Linux internal thread id
    static int getCurrentTID();

    ~TaskStatsInfoGetter();

private:

    int netlink_socket_fd = -1;
    int netlink_family_id = 0;
    int initial_tid = -1;
};

}
