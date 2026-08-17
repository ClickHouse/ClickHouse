#pragma once

#include <unordered_map>
#include <Processors/QueryPlan/StepStatsModel.h>
#include <Processors/Executors/WorkInterval.h>
#include <Processors/QueryPlan/ConcurrencyProfile.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/TimeIntervals.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

/// Attributes the work intervals collected during execution to query plan steps. Per step it gives
/// the wall-clock time of the step itself and of its whole subtree, and, over the same two sets of
/// intervals, the average number of threads the query kept busy while they were active.
class StepIntervalTimings
{
public:
    StepIntervalTimings(const WorkIntervalsPerThread & intervals_per_thread, const QueryPlan & plan);

    const StepTimeAndConcurrency * findTiming(const IQueryPlanStep * step) const;

private:

    using TimeIntervalsByStep = std::unordered_map<const IQueryPlanStep *, TimeIntervals>;

    /// Traverse the plan to get all the steps
    void collectPlanSteps(const QueryPlan &);

    /// One sorted, non-overlapping sequence per step, merged from the per-thread runs.
    TimeIntervalsByStep collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const;

    /// Post-order walk that records the metrics of every node from its own and its subtree's intervals.
    void computeBranchTime(const QueryPlan & plan, TimeIntervalsByStep time_intervals_by_step);

    std::vector<TimeIntervals> collectLowerBranchIntervals(TimeIntervals && current_step_intervals, const std::vector<QueryPlan::Node *> & children, const std::vector<QueryPlan *> & child_plans, TimeIntervalsByStep & branch_intervals_by_step) const;

    std::unordered_map<const IQueryPlanStep *, StepTimeAndConcurrency> timing_by_step;
    ConcurrencyProfile concurrency_profile;
};

}
