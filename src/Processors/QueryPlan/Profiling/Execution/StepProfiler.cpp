#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace
{

StepWallClocks createWallClocksForPlanSteps(const QueryPlan & plan, UInt64 origin_ns)
{
    StepWallClocks clocks;

    std::vector<const QueryPlan::Node *> stack;
    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * cur = stack.back();
        stack.pop_back();

        if (!cur)
            continue;

        for (size_t group : cur->step->getStepGroups())
            clocks.try_emplace(std::make_pair(cur->step.get(), group), std::make_unique<StepWallClock>(origin_ns));

        for (const auto * child : cur->children)
            stack.push_back(child);
        for (const auto * child_plan : cur->step->getChildPlans())
            stack.push_back(child_plan->getRootNode());
    }

    return clocks;
}

}

StepProfiler::StepProfiler(const QueryPlan & plan, bool collect_work_intervals_, UInt64 origin_ns_)
    : origin_ns(origin_ns_)
    , collect_work_intervals(collect_work_intervals_)
    , clocks(createWallClocksForPlanSteps(plan, origin_ns))
{
}

StepWallClock * StepProfiler::findClock(const IQueryPlanStep * step, size_t group) const
{
    auto it = clocks.find({step, group});
    return it != clocks.end() ? it->second.get() : nullptr;
}

bool StepProfiler::needCollectWorkIntervals() const
{
    return collect_work_intervals;
}

void StepProfiler::addWorkIntervals(WorkIntervals intervals)
{
    if (intervals.empty())
        return;

    for (auto & interval : intervals)
        interval.start_of_interval_ns -= origin_ns;

    std::lock_guard lock(mutex);
    intervals_per_thread.push_back(std::move(intervals));
}

WorkIntervalsPerThread StepProfiler::extractWorkIntervals()
{
    std::lock_guard lock(mutex);
    return std::move(intervals_per_thread);
}

}
