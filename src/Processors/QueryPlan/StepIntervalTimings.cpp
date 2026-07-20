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
    branch_sequences.push_back(collapseSorted(std::move(own)));
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

StepIntervalTimings::Intervals StepIntervalTimings::collapseSorted(Intervals sorted)
{
    size_t write = 0;
    for (size_t read = 0; read < sorted.size(); ++read)
    {
        if (write > 0 && sorted[read].start <= sorted[write - 1].end)
            sorted[write - 1].end = std::max(sorted[write - 1].end, sorted[read].end);
        else
            sorted[write++] = sorted[read];
    }
    sorted.resize(write);
    return sorted;
}

StepIntervalTimings::Intervals StepIntervalTimings::mergeSortedSequences(const std::vector<Intervals> & sorted_sequences)
{
    /// Cursor into one input sequence: which sequence, and the next unread position in it.
    struct Cursor
    {
        size_t sequence;
        size_t position;
    };

    size_t total = 0;
    for (const auto & sequence : sorted_sequences)
        total += sequence.size();

    /// Min-heap over the current head of each non-empty sequence, ordered by interval start.
    const auto later_start = [&](const Cursor & lhs, const Cursor & rhs)
    {
        return sorted_sequences[lhs.sequence][lhs.position].start
             > sorted_sequences[rhs.sequence][rhs.position].start;
    };

    std::vector<Cursor> heads;
    heads.reserve(sorted_sequences.size());
    for (size_t i = 0; i < sorted_sequences.size(); ++i)
        if (!sorted_sequences[i].empty())
            heads.push_back({i, 0});
    std::make_heap(heads.begin(), heads.end(), later_start);

    Intervals merged;
    merged.reserve(total);
    while (!heads.empty())
    {
        std::pop_heap(heads.begin(), heads.end(), later_start);
        Cursor & head = heads.back();
        merged.push_back(sorted_sequences[head.sequence][head.position]);

        if (++head.position < sorted_sequences[head.sequence].size())
            std::push_heap(heads.begin(), heads.end(), later_start);
        else
            heads.pop_back();
    }

    return merged;
}

StepIntervalTimings::Intervals StepIntervalTimings::uniteSortedSequences(const std::vector<Intervals> & sorted_sequences)
{
    return collapseSorted(mergeSortedSequences(sorted_sequences));
}

UInt64 StepIntervalTimings::totalLength(const Intervals & intervals)
{
    UInt64 total_length_ns = 0;
    for (const auto & interval : intervals)
        total_length_ns += interval.end - interval.start;
    return total_length_ns;
}

}
