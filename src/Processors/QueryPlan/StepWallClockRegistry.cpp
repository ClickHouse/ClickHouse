#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Stopwatch.h>
#include <cstddef>
#include <ctime>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StepWallClockRegistry::StepWallClockRegistry()
: query_start_ns(clock_gettime_ns())
{
}

void StepWallClockRegistry::populateFromPlan(const QueryPlan & plan)
{
    std::vector<const QueryPlan::Node *> stack;
    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * cur = stack.back();
        stack.pop_back();

        if (!cur)
            continue;

        for (size_t group : cur->step->getStepGroups())
        {
            auto key = std::make_pair(cur->step.get(), group);
            clocks.try_emplace(key, std::make_unique<StepWallClock>(query_start_ns));
        }

        for (const auto * child : cur->children)
            stack.push_back(child);
        for (const auto * child_plan : cur->step->getChildPlans())
            stack.push_back(child_plan->getRootNode());
    }
}

StepWallClock & StepWallClockRegistry::get(const IQueryPlanStep * step_ptr, size_t group) const
{
    auto key = std::make_pair(step_ptr, group);

    if (auto clock_it = clocks.find(key); clock_it != clocks.end())
        return *(clock_it->second);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "The clocks for measuring wall clock time for step {} with group {} were not found.", step_ptr->getName(), step_ptr->getStepGroupName(group));
}

const StepWallClock * StepWallClockRegistry::find(const IQueryPlanStep * step_ptr, size_t group) const
{
    auto it = clocks.find({step_ptr, group});
    return it != clocks.end() ? it->second.get() : nullptr;
}
    
}
