#include <Core/Types.h>
#include <Processors/QueryPlan/StepIntervalTimings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/TimeIntervals.h>

#include <deque>
#include <limits>
#include <unordered_map>

namespace DB
{

namespace
{

struct Frame
{
    const QueryPlan::Node * node = nullptr;
    std::vector<QueryPlan *> child_plans;
    size_t next_child = 0;
    size_t next_child_plan = 0;
};

double ratio(UInt64 busy_ns, UInt64 total_ns)
{
    return total_ns != 0 ? static_cast<double>(busy_ns) / static_cast<double>(total_ns) : 0.0;
}

const QueryPlan::Node * nextChildToVisit(Frame & current_frame)
{
    if (current_frame.next_child < current_frame.node->children.size())
    {
        auto * next_child = current_frame.node->children[current_frame.next_child];
        ++current_frame.next_child;
        return next_child;
    }

    while (current_frame.next_child_plan < current_frame.child_plans.size())
    {
        auto * child_plan = current_frame.child_plans[current_frame.next_child_plan];
        ++current_frame.next_child_plan;
        if (child_plan && child_plan->isInitialized())
        {
            return child_plan->getRootNode();
        }
    }
    return nullptr;
}

}

StepIntervalTimings::StepIntervalTimings(const WorkIntervalsPerThread & intervals_per_thread, const QueryPlan & plan)
    : concurrency_profile(intervals_per_thread)
{
    collectPlanSteps(plan);
    computeBranchTime(plan, collectStepIntervals(intervals_per_thread));
}

const StepTimeAndConcurrency * StepIntervalTimings::findTiming(const IQueryPlanStep * step) const
{
    const auto it = timing_by_step.find(step);
    return it != timing_by_step.end() ? &it->second : nullptr;
}

void StepIntervalTimings::collectPlanSteps(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return;

    std::vector<const QueryPlan::Node *> stack;

    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();

        timing_by_step.try_emplace(current->step.get());

        for (const auto * child : current->children)
            stack.push_back(child);

        for (const auto * child_plan : current->step->getChildPlans())
        {
            if (child_plan && child_plan->isInitialized())
                stack.push_back(child_plan->getRootNode());
        }
    }
}

StepIntervalTimings::TimeIntervalsByStep StepIntervalTimings::collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const
{
    static constexpr size_t no_thread = std::numeric_limits<size_t>::max();

    struct StepIntervalsByThread
    {
        std::vector<TimeIntervals> intervals_by_thread;
        size_t last_thread = no_thread;
    };

    std::unordered_map<const IQueryPlanStep *, StepIntervalsByThread> step_to_thread_intervals;
    step_to_thread_intervals.reserve(timing_by_step.size());

    for (const auto & [step, _] : timing_by_step)
        step_to_thread_intervals.try_emplace(step);

    for (size_t thread = 0; thread < intervals_per_thread.size(); ++thread)
    {
        for (const auto & interval : intervals_per_thread[thread])
        {
            const auto * step_ptr = interval.step;
            auto it = step_to_thread_intervals.find(step_ptr);

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
    for (const auto & [step, runs] : step_to_thread_intervals)
        time_intervals_by_step[step] = uniteSortedIntervals(runs.intervals_by_thread);

    return time_intervals_by_step;
}

void StepIntervalTimings::computeBranchTime(const QueryPlan & plan, TimeIntervalsByStep time_intervals_by_step)
{
    if (!plan.isInitialized())
        return;

    /// We use deque in order for the reference to the last element does not get invalidated during push back
    std::deque<Frame> stack;

    const auto push_frame = [&](const QueryPlan::Node * node)
    {
        Frame frame;
        frame.node = node;

        const auto child_plans = node->step->getChildPlans();
        frame.child_plans.assign(child_plans.begin(), child_plans.end());

        stack.push_back(std::move(frame));
    };

    push_frame(plan.getRootNode());

    TimeIntervalsByStep branch_intervals_by_step;

    while (!stack.empty())
    {
        Frame & frame = stack.back();

        if (const auto * next_child = nextChildToVisit(frame))
        {
            push_frame(next_child);
            continue;
        }

        const auto * current_step = frame.node->step.get();
        /// Current Step Intervals gets moved, so we compute the total time and concurrency for the step first
        TimeIntervals current_step_intervals = std::move(time_intervals_by_step.at(current_step));
        StepTimeAndConcurrency & timing = timing_by_step.at(current_step);

        timing.step_time_ns = totalIntervalsLength(current_step_intervals);
        timing.step_concurrency = ratio(concurrency_profile.busyTimeIn(current_step_intervals), timing.step_time_ns);

        /// Compute the Branch time and concurrency
        auto current_step_branch_intervals = collectLowerBranchIntervals(std::move(current_step_intervals), frame.node->children, frame.child_plans, branch_intervals_by_step);
        TimeIntervals branch_time = uniteSortedIntervals(current_step_branch_intervals);

        timing.branch_time_ns = totalIntervalsLength(branch_time);
        timing.branch_concurrency = ratio(concurrency_profile.busyTimeIn(branch_time), timing.branch_time_ns);

        branch_intervals_by_step[current_step] = std::move(branch_time);

        stack.pop_back();
    }
}

std::vector<TimeIntervals> StepIntervalTimings::collectLowerBranchIntervals(TimeIntervals && current_step_intervals, const std::vector<QueryPlan::Node *> & children, const std::vector<QueryPlan *> & child_plans, TimeIntervalsByStep & branch_intervals_by_step) const
{
    std::vector<TimeIntervals> current_step_branch_intervals;

    current_step_branch_intervals.push_back(std::move(current_step_intervals));

    for (const auto * child : children)
    {
        auto child_branch_timing = std::move(branch_intervals_by_step.at(child->step.get()));
        current_step_branch_intervals.push_back(std::move(child_branch_timing));
    }

    for (const auto * child_plan : child_plans)
    {
        if (!child_plan || !child_plan->isInitialized())
            continue;

        const auto * plan_root_ptr = child_plan->getRootNode()->step.get();
        auto child_plan_branch_timing = std::move(branch_intervals_by_step.at(plan_root_ptr));
        current_step_branch_intervals.push_back(std::move(child_plan_branch_timing));
    }

    return current_step_branch_intervals;
}

}
