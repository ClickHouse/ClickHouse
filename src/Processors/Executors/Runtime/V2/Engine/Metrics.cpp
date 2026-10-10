#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>

namespace DB
{

namespace Runtime::V2
{

Metrics combine(const Metrics & lhs, const Metrics & rhs)
{
    return Metrics
    {
        .local_picks = lhs.local_picks + rhs.local_picks,
        .global_picks = lhs.global_picks + rhs.global_picks,
        .poll_picks = lhs.poll_picks + rhs.poll_picks,
        .steal_picks = lhs.steal_picks + rhs.steal_picks,
        .steal_misses = lhs.steal_misses + rhs.steal_misses,
        .offloads = lhs.offloads + rhs.offloads,
        .parks = lhs.parks + rhs.parks,
        .wakes = lhs.wakes + rhs.wakes,
        .wake_skips = lhs.wake_skips + rhs.wake_skips,
    };
}

}

}
