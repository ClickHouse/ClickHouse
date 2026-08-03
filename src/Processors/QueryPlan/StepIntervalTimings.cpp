#include <Processors/QueryPlan/StepIntervalTimings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <limits>

namespace DB
{

namespace
{

double ratio(UInt64 busy_ns, UInt64 total_ns)
{
    return total_ns != 0 ? static_cast<double>(busy_ns) / static_cast<double>(total_ns) : 0.0;
}

}

StepIntervalTimings::StepIntervalTimings(const WorkIntervalsPerThread & intervals_per_thread, const QueryPlan & plan)
    : concurrency_profile(intervals_per_thread)
{
    indexPlanSteps(plan);
    computeBranchTime(plan, collectStepIntervals(intervals_per_thread));
}

UInt64 StepIntervalTimings::getStepTime(const IQueryPlanStep * step) const
{
    const auto * timing = findTiming(step);
    return timing ? timing->step_time_ns : 0;
}

UInt64 StepIntervalTimings::getBranchTime(const IQueryPlanStep * step) const
{
    const auto * timing = findTiming(step);
    return timing ? timing->branch_time_ns : 0;
}

double StepIntervalTimings::getStepConcurrency(const IQueryPlanStep * step) const
{
    const auto * timing = findTiming(step);
    return timing ? timing->step_concurrency : 0.0;
}

double StepIntervalTimings::getBranchConcurrency(const IQueryPlanStep * step) const
{
    const auto * timing = findTiming(step);
    return timing ? timing->branch_concurrency : 0.0;
}

const StepIntervalTimings::StepTiming * StepIntervalTimings::findTiming(const IQueryPlanStep * step) const
{
    const auto it = index_by_step.find(step);
    return it != index_by_step.end() ? &timings[it->second] : nullptr;
}

void StepIntervalTimings::indexPlanSteps(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return;

    std::vector<QueryPlan::Node *> stack;
    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        index_by_step.emplace(node->step.get(), index_by_step.size());

        for (auto * child : node->children)
            stack.push_back(child);

        for (auto * child_plan : node->step->getChildPlans())
            if (child_plan && child_plan->isInitialized())
                stack.push_back(child_plan->getRootNode());
    }

    timings.resize(index_by_step.size());
}

std::vector<TimeIntervals> StepIntervalTimings::collectStepIntervals(const WorkIntervalsPerThread & intervals_per_thread) const
{
    static constexpr size_t no_thread = std::numeric_limits<size_t>::max();

    std::vector<std::vector<TimeIntervals>> runs_by_step(timings.size());
    std::vector<size_t> last_thread(timings.size(), no_thread);

    for (size_t thread = 0; thread < intervals_per_thread.size(); ++thread)
    {
        for (const auto & interval : intervals_per_thread[thread])
        {
            const auto it = index_by_step.find(interval.step);
            if (it == index_by_step.end())
                continue;

            const size_t step_index = it->second;
            if (last_thread[step_index] != thread)
            {
                runs_by_step[step_index].emplace_back();
                last_thread[step_index] = thread;
            }

            runs_by_step[step_index].back().push_back(
                {interval.start_of_interval_ns, interval.start_of_interval_ns + interval.duration_of_interval_ns});
        }
    }

    std::vector<TimeIntervals> step_intervals(timings.size());
    for (size_t step_index = 0; step_index < step_intervals.size(); ++step_index)
        step_intervals[step_index] = uniteSortedIntervals(runs_by_step[step_index]);

    return step_intervals;
}

void StepIntervalTimings::computeBranchTime(const QueryPlan & plan, std::vector<TimeIntervals> step_intervals)
{
    if (!plan.isInitialized())
        return;

    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        std::vector<QueryPlan *> child_plans;
        size_t next_child = 0;
        size_t next_child_plan = 0;
        /// The node's own intervals first, then the branch of every child that has finished.
        std::vector<TimeIntervals> branch_sequences;
    };

    std::vector<Frame> stack;

    const auto push_frame = [&](QueryPlan::Node * node)
    {
        Frame frame;
        frame.node = node;

        const auto child_plans = node->step->getChildPlans();
        frame.child_plans.assign(child_plans.begin(), child_plans.end());

        TimeIntervals own;
        if (const auto it = index_by_step.find(node->step.get()); it != index_by_step.end())
            own = std::move(step_intervals[it->second]);
        frame.branch_sequences.push_back(std::move(own));

        stack.push_back(std::move(frame));
    };

    push_frame(plan.getRootNode());

    TimeIntervals branch_of_child;
    bool has_branch_of_child = false;

    while (!stack.empty())
    {
        Frame & frame = stack.back();

        if (has_branch_of_child)
        {
            frame.branch_sequences.push_back(std::move(branch_of_child));
            has_branch_of_child = false;
        }

        if (frame.next_child < frame.node->children.size())
        {
            push_frame(frame.node->children[frame.next_child++]);
            continue;
        }

        bool descended = false;
        while (frame.next_child_plan < frame.child_plans.size())
        {
            auto * child_plan = frame.child_plans[frame.next_child_plan++];
            if (child_plan && child_plan->isInitialized())
            {
                push_frame(child_plan->getRootNode());
                descended = true;
                break;
            }
        }
        if (descended)
            continue;

        TimeIntervals branch = uniteSortedIntervals(frame.branch_sequences);

        if (const auto it = index_by_step.find(frame.node->step.get()); it != index_by_step.end())
        {
            const TimeIntervals & own = frame.branch_sequences.front();
            StepTiming & timing = timings[it->second];

            timing.step_time_ns = totalIntervalsLength(own);
            timing.branch_time_ns = totalIntervalsLength(branch);
            timing.step_concurrency = ratio(concurrency_profile.busyTimeIn(own), timing.step_time_ns);
            timing.branch_concurrency = ratio(concurrency_profile.busyTimeIn(branch), timing.branch_time_ns);
        }

        branch_of_child = std::move(branch);
        has_branch_of_child = true;
        stack.pop_back();
    }
}

}
