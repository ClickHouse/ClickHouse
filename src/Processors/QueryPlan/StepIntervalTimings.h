#pragma once

#include <unordered_map>
#include <vector>
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

    UInt64 getStepTime(const IQueryPlanStep * step) const;
    UInt64 getBranchTime(const IQueryPlanStep * step) const;
    double getStepConcurrency(const IQueryPlanStep * step) const;
    double getBranchConcurrency(const IQueryPlanStep * step) const;

private:
    struct StepTiming
    {
        UInt64 step_time_ns = 0;
        UInt64 branch_time_ns = 0;
        double step_concurrency = 0.0;
        double branch_concurrency = 0.0;
    };

    /// Give every step of the plan tree a unique number,
    /// so that it is easier to access
    void indexPlanSteps(const QueryPlan & plan);

    /// One sorted, non-overlapping sequence per step, merged from the per-thread runs.
    std::vector<TimeIntervals> collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const;

    /// Post-order walk that records the metrics of every node from its own and its subtree's intervals.
    void computeBranchTime(const QueryPlan & plan, std::vector<TimeIntervals> step_intervals);

    const StepTiming * findTiming(const IQueryPlanStep * step) const;

    std::unordered_map<const IQueryPlanStep *, size_t> index_by_step;
    std::vector<StepTiming> timings;
    ConcurrencyProfile concurrency_profile;
};

}
