#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>

namespace DB
{

namespace Runtime::V2
{

void addCounters(PoolMetrics & pool, const WorkerMetrics & worker)
{
    pool.workers.local_picks += worker.local_picks;
    pool.workers.global_picks += worker.global_picks;
    pool.workers.steal_picks += worker.steal_picks;
    pool.workers.stolen_records += worker.stolen_records;
    pool.workers.steal_misses += worker.steal_misses;
    pool.workers.offloads += worker.offloads;
    pool.workers.parks += worker.parks;
    pool.workers.wakes += worker.wakes;
    pool.workers.wake_skips += worker.wake_skips;
    pool.workers.spawns += worker.spawns;
    pool.workers.no_op_wakes += worker.no_op_wakes;
}

void addCounters(PoolMetrics & pool, const PollerMetrics & poller)
{
    pool.poller.polls += poller.polls;
    pool.poller.fired += poller.fired;
    pool.poller.wakes += poller.wakes;
    pool.poller.wake_skips += poller.wake_skips;
    pool.poller.spawns += poller.spawns;
    pool.poller.no_op_wakes += poller.no_op_wakes;
}

}

}
