#include <Processors/QueryPlan/Profiling/Capture/PlanWalk.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

PlanWalk PlanWalk::of(const QueryPlan & plan, bool only_built_child_plans)
{
    PlanWalk walk;

    if (!plan.isInitialized())
        return walk;

    auto * root = plan.getRootNode();
    if (!root || !root->step || root->step->getStepIndex() == PlanStepIndex::NONE)
        return walk;

    walk.plan = &plan;
    walk.root_id = root->step->getStepIndex();

    struct Frame
    {
        const QueryPlan * owning_plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    VectorWithMemoryTracking<Frame> to_visit{{&plan, root}};

    /// The children of a node: its own, then the roots of the child plans its step holds.
    const auto for_each_child = [only_built_child_plans](const Frame & frame, const auto & callback)
    {
        for (auto * child : frame.node->children)
        {
            if (child && child->step)
                callback(Frame{frame.owning_plan, child});
        }

        auto & step = *frame.node->step;
        for (auto * child_plan : only_built_child_plans ? step.getBuiltChildPlans() : step.getChildPlans(/*for_explain=*/ false))
        {
            if (!child_plan || !child_plan->isInitialized())
                continue;
            auto * child_root = child_plan->getRootNode();
            if (child_root && child_root->step)
                callback(Frame{child_plan, child_root});
        }
    };

    /// A step with `PlanStepIndex::NONE` was built outside of any query and has no identity to key
    /// it by: it would collide with every other such step. It is left out of the walk, and the
    /// steps below it hang from the nearest step above it instead.
    const auto attach = [&](this auto && self, VisitedStep & parent, const Frame & child) -> void
    {
        const UInt64 child_id = child.node->step->getStepIndex();
        if (child_id != PlanStepIndex::NONE)
        {
            parent.children.push_back(child_id);
            to_visit.push_back(child);
            return;
        }

        for_each_child(child, [&](const Frame & grandchild) { self(parent, grandchild); });
    };

    while (!to_visit.empty())
    {
        const auto frame = to_visit.back();
        to_visit.pop_back();

        auto & step = *frame.node->step;

        VisitedStep visited;
        visited.id = step.getStepIndex();
        visited.step = &step;
        visited.owning_plan = frame.owning_plan;

        for_each_child(frame, [&](const Frame & child) { attach(visited, child); });

        walk.steps.push_back(std::move(visited));
    }

    return walk;
}

}
