#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

namespace DB
{

PlanWalk PlanWalk::of(const QueryPlan & plan, bool only_built_child_plans)
{
    PlanWalk walk;

    if (!plan.isInitialized())
        return walk;

    auto * root = plan.getRootNode();
    if (!root || !root->step)
        return walk;

    walk.plan = &plan;
    walk.root_id = root->step->getStepIndex();

    struct Frame
    {
        const QueryPlan * owning_plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    VectorWithMemoryTracking<Frame> to_visit{{&plan, root}};

    /// The step each index was found at. A step is reached again where a child plan is held by more
    /// than one step, and is not walked twice.
    UnorderedMapWithMemoryTracking<UInt64, const IQueryPlanStep *> step_by_index;

    while (!to_visit.empty())
    {
        const auto frame = to_visit.back();
        to_visit.pop_back();

        auto & step = *frame.node->step;
        const UInt64 index = step.getStepIndex();

        /// A step built outside of any query has no identity to key it by: it would collide with
        /// every other such step.
        if (index == PlanStepIndex::NONE)
        {
            PlanWalk empty;
            empty.has_step_without_index = true;
            return empty;
        }

        const auto [it, inserted] = step_by_index.try_emplace(index, &step);
        if (!inserted)
        {
            if (it->second == &step)
                continue;

            /// The steps of a query are numbered by one counter, so this is a bug. Keyed by the
            /// index, the plan could make a step its own descendant, and reading it would not end.
            chassert(false, "two steps of a plan have the same index");
            return {};
        }

        VisitedStep visited;
        visited.id = index;
        visited.step = &step;
        visited.owning_plan = frame.owning_plan;

        for (auto * child : frame.node->children)
        {
            if (!child || !child->step)
                continue;
            visited.children.push_back(child->step->getStepIndex());
            to_visit.push_back({frame.owning_plan, child});
        }

        for (auto * child_plan : only_built_child_plans ? step.getBuiltChildPlans() : step.getChildPlans(/*for_explain=*/ false))
        {
            if (!child_plan || !child_plan->isInitialized())
                continue;
            auto * child_root = child_plan->getRootNode();
            if (!child_root || !child_root->step)
                continue;
            visited.children.push_back(child_root->step->getStepIndex());
            to_visit.push_back({child_plan, child_root});
        }

        walk.steps.push_back(std::move(visited));
    }

    return walk;
}

}
