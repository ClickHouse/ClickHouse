#include <Processors/QueryPlan/StepIntervalTimings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/IProcessor.h>
#include <algorithm>
#include <iterator>

namespace DB
{

StepIntervalTimings::StepIntervalTimings(const WorkIntervals & intervals, const QueryPlan & plan)
{
    collectStepIntervals(intervals);

    if (plan.isInitialized())
        computeBranchTime(plan.getRootNode());
}

UInt64 StepIntervalTimings::getStepTime(const IQueryPlanStep * step) const
{
    const auto it = timings_by_step.find(step);
    return it != timings_by_step.end() ? it->second.step_time_ns : 0;
}

UInt64 StepIntervalTimings::getBranchTime(const IQueryPlanStep * step) const
{
    const auto it = timings_by_step.find(step);
    return it != timings_by_step.end() ? it->second.branch_time_ns : 0;
}

void StepIntervalTimings::collectStepIntervals(const WorkIntervals & intervals)
{
    for (const auto & interval : intervals)
    {
        const auto * step = interval.processor->getQueryPlanStep();
        if (!step)
            continue;

        timings_by_step[step].intervals.push_back(
            {interval.start_of_interval_ns, interval.start_of_interval_ns + interval.duration_of_interval_ns});
    }
}

StepIntervalTimings::Intervals StepIntervalTimings::computeBranchTime(QueryPlan::Node * node)
{
    const auto * step = node->step.get();

    Intervals own = std::move(timings_by_step[step].intervals);
    std::sort(own.begin(), own.end(),
        [](const Interval & lhs, const Interval & rhs) { return lhs.start < rhs.start; });

    std::vector<Intervals> branch_sequences;
    branch_sequences.push_back(uniteSortedSequences({own}));
    const UInt64 step_time_ns = totalLength(branch_sequences.front());

    for (auto * child : node->children)
        branch_sequences.push_back(computeBranchTime(child));

    for (auto * child_plan : node->step->getChildPlans())
        if (child_plan && child_plan->isInitialized())
            branch_sequences.push_back(computeBranchTime(child_plan->getRootNode()));

    Intervals branch = uniteSortedSequences(branch_sequences);

    /// Look the step up again: the recursion above may have rehashed the map.
    auto & timing = timings_by_step[step];
    timing.step_time_ns = step_time_ns;
    timing.branch_time_ns = totalLength(branch);

    return branch;
}

StepIntervalTimings::Intervals StepIntervalTimings::uniteSortedSequences(const std::vector<Intervals> & sorted_sequences)
{
    Intervals merged;
    for (const auto & sequence : sorted_sequences)
    {
        Intervals next;
        next.reserve(merged.size() + sequence.size());
        std::merge(merged.begin(), merged.end(), sequence.begin(), sequence.end(), std::back_inserter(next),
            [](const Interval & lhs, const Interval & rhs) { return lhs.start < rhs.start; });
        merged = std::move(next);
    }

    Intervals united;
    for (const auto & interval : merged)
    {
        if (!united.empty() && interval.start <= united.back().end)
            united.back().end = std::max(united.back().end, interval.end);
        else
            united.push_back(interval);
    }

    return united;
}

UInt64 StepIntervalTimings::totalLength(const Intervals & intervals)
{
    UInt64 total_length_ns = 0;
    for (const auto & interval : intervals)
        total_length_ns += interval.end - interval.start;
    return total_length_ns;
}

}
