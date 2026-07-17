#pragma once

#include <unordered_map>
#include <vector>
#include <Processors/Executors/WorkInterval.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>

namespace DB
{

class IQueryPlanStep;

/// Computes, from the work intervals collected during execution, per step:
///  - step time: length of the union of the intervals of the processors the step owns;
///  - branch time: length of that union taken over the step's whole subtree in the plan tree,
///    including embedded child plans.
/// The union discounts the overlap of intervals running in parallel, so both are wall-clock times.
class StepIntervalTimings
{
public:
    StepIntervalTimings(const WorkIntervals & intervals, const QueryPlan & plan);

    UInt64 getStepTime(const IQueryPlanStep * step) const;
    UInt64 getBranchTime(const IQueryPlanStep * step) const;

private:
    struct Interval
    {
        UInt64 start;
        UInt64 end;
    };
    using Intervals = std::vector<Interval>;

    struct StepTiming
    {
        /// The step's own intervals, working data used only while walking the tree.
        Intervals intervals;
        UInt64 step_time_ns = 0;
        UInt64 branch_time_ns = 0;
    };

    using StepTimings = std::unordered_map<const IQueryPlanStep *, StepTiming>;

    /// Bucket every work interval under the step of the processor that produced it.
    void collectStepIntervals(const WorkIntervals & intervals);

    /// Post-order walk that records the step and branch time of the node's step and returns the
    /// branch's intervals as one sorted, non-overlapping sequence for the parent to reuse.
    Intervals computeBranchTime(QueryPlan::Node * node);

    /// Merge sorted sequences and collapse overlaps into one sorted, non-overlapping sequence.
    static Intervals uniteSortedSequences(const std::vector<Intervals> & sorted_sequences);

    /// Total length of a non-overlapping sequence.
    static UInt64 totalLength(const Intervals & intervals);

    StepTimings timings_by_step;
};

}
