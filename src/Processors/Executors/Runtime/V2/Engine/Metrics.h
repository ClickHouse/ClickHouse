#pragma once

#include <base/types.h>

namespace DB
{

namespace Runtime::V2
{

struct Metrics
{
    UInt64 local_picks = 0;
    UInt64 global_picks = 0;
    UInt64 poll_picks = 0;
    UInt64 steal_picks = 0;
    UInt64 steal_misses = 0;
    UInt64 offloads = 0;
    UInt64 parks = 0;
    UInt64 wakes = 0;
    UInt64 wake_skips = 0;
};

Metrics combine(const Metrics & lhs, const Metrics & rhs);

}

}
