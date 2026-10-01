#pragma once

#include <vector>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>
#include <Processors/QueryPlan/Profiling/Time/TimeIntervals.h>
#include <base/types.h>

namespace DB
{

/// A prefix sum of squares of intervals, which is built using:
///  - concurrency (y-axis) -- number of threads running simultaniously at the moment of time
///  - times (x-axis) -- time slots related to the beginning of interval
///  - busy_integral -- prefix sum where busy_integral[x + 1] = busy_integral[x] + concurrency[x + 1] * times[x + 1]
///  - active_time_ns -- total length of the slots where the concurrency is above zero
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

    /// Length of the union of all intervals: the time during which at least one thread was busy.
    UInt64 activeTime() const { return active_time_ns; }

private:
    UInt64 integralAt(UInt64 time) const;

    VectorWithMemoryTracking<UInt64> times;
    VectorWithMemoryTracking<UInt64> concurrency;
    VectorWithMemoryTracking<UInt64> busy_integral;
    UInt64 active_time_ns = 0;
};

}
