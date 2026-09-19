#pragma once

#include <vector>
#include <Processors/Executors/WorkInterval.h>
#include <Processors/QueryPlan/TimeIntervals.h>
#include <base/types.h>

namespace DB
{

/// A prefix sum of squares of intervals, which is built using:
///  - concurrency (y-axis) -- number of threads running simultaniously at the moment of time
///  - times (x-axis) -- time slots related to the beginning of interval
///  - busy_integral -- prefix sum where busy_integral[x + 1] = busy_integral[x] + concurrency[x + 1] * times[x + 1]
/// c(t)
/// 2 |         ┌─────┐
/// 1 |  ┌──────┘     └───────────┐
/// 0 | ─┘                        └────
/// ──┴──────┴────────┴───────────┴─────→ t
///   0      5       10         20
class ConcurrencyProfile
{
public:
    explicit ConcurrencyProfile(const WorkIntervalsPerThread & intervals_per_thread);

    /// Time-weighted number of busy threads over the given non-overlapping sequence
    UInt64 busyTimeIn(const TimeIntervals & intervals) const;

private:
    UInt64 integralAt(UInt64 time) const;

    std::vector<UInt64> times;
    std::vector<UInt64> concurrency;
    std::vector<UInt64> busy_integral;
};

}
