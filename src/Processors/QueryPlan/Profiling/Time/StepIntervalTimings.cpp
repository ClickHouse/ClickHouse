#include <Core/Types.h>
#include <base/defines.h>
#include <Processors/QueryPlan/Profiling/Time/StepIntervalTimings.h>
#include <Processors/QueryPlan/Profiling/Time/TimeIntervals.h>

#include <limits>
#include <optional>
#include <unordered_map>

namespace DB
{

namespace
{

/// Average number of busy threads over intervals of total length `total_ns`.
/// Empty when there were no intervals: a step that did no work has no concurrency.
std::optional<double> ratio(UInt64 busy_ns, UInt64 total_ns)
{
    if (total_ns == 0)
        return std::nullopt;
    return static_cast<double>(busy_ns) / static_cast<double>(total_ns);
}

}

StepIntervalTimings::StepIntervalTimings(const WorkIntervalsPerThread & intervals_per_thread, const PlanAnalysisSnapshot & plan_snapshot)
    : concurrency_profile(intervals_per_thread)
{
    for (const auto & [step_id, _] : plan_snapshot.analyzers)
        timing_by_step.try_emplace(step_id);

    computeBranchTime(plan_snapshot, collectStepIntervals(intervals_per_thread));
}

const StepTimeAndConcurrency * StepIntervalTimings::findTiming(UInt64 step_id) const
{
    const auto it = timing_by_step.find(step_id);
    return it != timing_by_step.end() ? &it->second : nullptr;
}

ExecutionTimeBreakdown StepIntervalTimings::executionTimeBreakdown(UInt64 execution_time_ns) const
{
    const UInt64 active_time_ns = concurrency_profile.activeTime();

    /// The intervals of the plan steps are a subset of all intervals, and all intervals lie inside the
    /// execution: they are measured with the same monotonic clock, and the executor joins its threads
    /// before the execution time is read. So the subtractions cannot underflow.
    chassert(all_steps_time_ns <= active_time_ns);
    chassert(active_time_ns <= execution_time_ns);

    return {
        .in_steps_ns = all_steps_time_ns,
        .outside_steps_ns = active_time_ns - all_steps_time_ns,
        .idle_ns = execution_time_ns - active_time_ns,
    };
}

StepIntervalTimings::TimeIntervalsByStep StepIntervalTimings::collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const
{
    static constexpr size_t no_thread = std::numeric_limits<size_t>::max();

    struct StepIntervalsByThread
    {
        std::vector<TimeIntervals> intervals_by_thread;
        size_t last_thread = no_thread;
    };

    std::unordered_map<UInt64, StepIntervalsByThread> step_to_thread_intervals;
    step_to_thread_intervals.reserve(timing_by_step.size());

    for (const auto & [step_id, _] : timing_by_step)
        step_to_thread_intervals.try_emplace(step_id);

    for (size_t thread = 0; thread < intervals_per_thread.size(); ++thread)
    {
        for (const auto & interval : intervals_per_thread[thread])
        {
            auto it = step_to_thread_intervals.find(interval.step_id);

            if (it == step_to_thread_intervals.end())
                continue;

            auto & [per_thread_intervals, last_thread] = it->second;

            if (last_thread != thread)
            {
                per_thread_intervals.emplace_back();
                last_thread = thread;
            }

            per_thread_intervals.back().push_back(
                {interval.start_of_interval_ns, interval.start_of_interval_ns + interval.duration_of_interval_ns});
        }
    }

    TimeIntervalsByStep time_intervals_by_step;
    time_intervals_by_step.reserve(step_to_thread_intervals.size());
    for (const auto & [step_id, runs] : step_to_thread_intervals)
        time_intervals_by_step[step_id] = uniteSortedIntervals(runs.intervals_by_thread);

    return time_intervals_by_step;
}

void StepIntervalTimings::computeBranchTime(const PlanAnalysisSnapshot & plan_snapshot, TimeIntervalsByStep time_intervals_by_step)
{
    TimeIntervalsByStep branch_intervals_by_step;

    for (const auto * node : plan_snapshot.stepsInPostOrder())
    {
        /// The step's own intervals get moved into its branch, so its own metrics come first.
        TimeIntervals current_step_intervals = std::move(time_intervals_by_step.at(node->id));
        StepTimeAndConcurrency & timing = timing_by_step.at(node->id);

        timing.step_time_ns = totalIntervalsLength(current_step_intervals);
        timing.step_concurrency = ratio(concurrency_profile.busyTimeIn(current_step_intervals), timing.step_time_ns);

        auto current_step_branch_intervals
            = collectLowerBranchIntervals(std::move(current_step_intervals), node->children, branch_intervals_by_step);
        TimeIntervals branch_time = uniteSortedIntervals(current_step_branch_intervals);

        timing.branch_time_ns = totalIntervalsLength(branch_time);
        timing.branch_concurrency = ratio(concurrency_profile.busyTimeIn(branch_time), timing.branch_time_ns);

        branch_intervals_by_step[node->id] = std::move(branch_time);
    }

    /// The root is retired last, so its branch unites the intervals of every step of the plan.
    if (const auto root_it = timing_by_step.find(plan_snapshot.root_id); root_it != timing_by_step.end())
        all_steps_time_ns = root_it->second.branch_time_ns;
}

std::vector<TimeIntervals> StepIntervalTimings::collectLowerBranchIntervals(
    TimeIntervals && current_step_intervals, const std::vector<UInt64> & children, TimeIntervalsByStep & branch_intervals_by_step) const
{
    std::vector<TimeIntervals> current_step_branch_intervals;

    current_step_branch_intervals.push_back(std::move(current_step_intervals));

    for (const auto & child_id : children)
    {
        const auto it = branch_intervals_by_step.find(child_id);
        if (it == branch_intervals_by_step.end())
            continue;
        current_step_branch_intervals.push_back(std::move(it->second));
    }

    return current_step_branch_intervals;
}

}
