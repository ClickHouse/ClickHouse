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
    if (!root || !root->step)
        return walk;

    walk.plan = &plan;
    walk.root_id = root->step->getUniqID();

    struct Frame
    {
        const QueryPlan * owning_plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    std::vector<Frame> to_visit{{&plan, root}};

    while (!to_visit.empty())
    {
        const auto frame = to_visit.back();
        to_visit.pop_back();

        if (!frame.node || !frame.node->step)
            continue;

        auto & step = *frame.node->step;

        VisitedStep visited;
        visited.id = step.getUniqID();
        visited.step = &step;
        visited.owning_plan = frame.owning_plan;

        for (auto * child : frame.node->children)
        {
            if (!child || !child->step)
                continue;
            visited.children.push_back(child->step->getUniqID());
            to_visit.push_back({frame.owning_plan, child});
        }

        for (auto * child_plan : only_built_child_plans ? step.getBuiltChildPlans() : step.getChildPlans(/*for_explain=*/ false))
        {
            if (!child_plan || !child_plan->isInitialized())
                continue;
            auto * child_root = child_plan->getRootNode();
            if (!child_root || !child_root->step)
                continue;
            visited.children.push_back(child_root->step->getUniqID());
            to_visit.push_back({child_plan, child_root});
        }

        walk.steps.push_back(std::move(visited));
    }

    return walk;
}

}
