#pragma once

#include <vector>
#include <Processors/Executors/WorkInterval.h>
#include <Processors/QueryPlan/TimeIntervals.h>
#include <base/types.h>

namespace DB
{

/// The number of work intervals covering each instant, as a piecewise-constant function of time,
/// with its running integral so that the busy time over any range costs a binary search.
/// Since an executor thread runs one job at a time, that number is also the number of busy threads.
class ConcurrencyProfile
{
public:
    explicit ConcurrencyProfile(const WorkIntervalsPerThread & intervals_per_thread);

    /// Time-weighted number of busy threads over the given non-overlapping sequence, i.e. the
    /// integral of the concurrency function restricted to it.
    UInt64 busyTimeIn(const TimeIntervals & intervals) const;

private:
    UInt64 integralAt(UInt64 time) const;

    std::vector<UInt64> times;
    std::vector<UInt64> concurrency;
    std::vector<UInt64> busy_integral;
};

}
