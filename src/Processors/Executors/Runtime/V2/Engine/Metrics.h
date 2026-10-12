#pragma once

#include <base/types.h>

namespace DB
{

namespace Runtime::V2
{

struct WorkerMetrics
{
    UInt64 local_picks = 0;
    UInt64 global_picks = 0;
    UInt64 steal_picks = 0;
    UInt64 stolen_records = 0;
    UInt64 steal_misses = 0;
    UInt64 offloads = 0;
    UInt64 parks = 0;
    UInt64 wakes = 0;
    UInt64 wake_skips = 0;
    UInt64 spawns = 0;
    UInt64 no_op_wakes = 0;
};

struct PollerMetrics
{
    UInt64 polls = 0;
    UInt64 fired = 0;
    UInt64 wakes = 0;
    UInt64 wake_skips = 0;
    UInt64 spawns = 0;
    UInt64 no_op_wakes = 0;
};

struct PoolMetrics
{
    WorkerMetrics workers;
    PollerMetrics poller;
};

void addCounters(PoolMetrics & pool, const WorkerMetrics & worker);
void addCounters(PoolMetrics & pool, const PollerMetrics & poller);

}

}
