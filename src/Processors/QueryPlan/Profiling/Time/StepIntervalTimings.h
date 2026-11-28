#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Processors/QueryPlan/Profiling/Analysis/StepStatsModel.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>
#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>
#include <Processors/QueryPlan/Profiling/Time/ConcurrencyProfile.h>
#include <Processors/QueryPlan/Profiling/Time/TimeIntervals.h>
#include <base/types.h>

namespace DB
{

/// Attributes the work intervals collected during execution to query plan steps. Per step it gives
/// the wall-clock time of the step itself and of its whole subtree, and, over the same two sets of
/// intervals, the average number of threads the query kept busy while they were active.
/// A step that owns no intervals has a zero time and no concurrency.
/// For the query as a whole it splits the execution time into the part inside the steps of the plan,
/// the part inside processors that belong to no step of the plan, and the idle part.
class StepIntervalTimings
{
public:
    /// Takes the plan's shape from the snapshot, so it runs once the plan is gone.
    StepIntervalTimings(const WorkIntervalsPerThread & intervals_per_thread, const PlanAnalysisSnapshot & plan_snapshot);

    [[nodiscard]] const StepTimeAndConcurrency * findTiming(UInt64 step_id) const;

    /// `execution_time_ns` is the wall-clock time of the whole pipeline execution, the denominator of every time share.
    [[nodiscard]] ExecutionTimeBreakdown executionTimeBreakdown(UInt64 execution_time_ns) const;

private:

    using TimeIntervalsByStep = UnorderedMapWithMemoryTracking<UInt64, TimeIntervals>;

    /// One sorted, non-overlapping sequence per step, merged from the per-thread runs.
    [[nodiscard]] TimeIntervalsByStep collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const;

    /// Post-order walk that records the metrics of every node from its own and its subtree's intervals.
    void computeBranchTime(const PlanAnalysisSnapshot & plan_snapshot, TimeIntervalsByStep time_intervals_by_step);

    [[nodiscard]] VectorWithMemoryTracking<TimeIntervals> collectLowerBranchIntervals(
        TimeIntervals && current_step_intervals,
        const VectorWithMemoryTracking<UInt64> & children,
        TimeIntervalsByStep & branch_intervals_by_step) const;

    UnorderedMapWithMemoryTracking<UInt64, StepTimeAndConcurrency> timing_by_step;
    ConcurrencyProfile concurrency_profile;

    /// Length of the union of the intervals of every step of the plan: the branch time of the root step.
    UInt64 all_steps_time_ns = 0;
};

}
