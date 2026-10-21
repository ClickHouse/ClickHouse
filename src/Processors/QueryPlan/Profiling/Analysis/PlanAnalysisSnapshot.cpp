#include <Processors/QueryPlan/Profiling/Analysis/PlanAnalysisSnapshot.h>

#include <vector>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/typeid_cast.h>

namespace DB
{

PlanAnalysisSnapshot PlanAnalysisSnapshot::capture(const QueryPlan & plan)
{
    PlanAnalysisSnapshot snapshot;

    if (!plan.isInitialized())
        return snapshot;

    auto * root = plan.getRootNode();
    if (!root || !root->step)
        return snapshot;

    snapshot.root_id = root->step->getUniqID();

    std::vector<QueryPlan::Node *> to_visit{root};
    while (!to_visit.empty())
    {
        auto * node = to_visit.back();
        to_visit.pop_back();
        if (!node || !node->step)
            continue;

        auto & step = *node->step;
        const String step_id = step.getUniqID();

        JoinBranchNode branch_node;
        branch_node.id = step_id;

        for (auto * child : node->children)
        {
            if (!child || !child->step)
                continue;
            branch_node.children.push_back(child->step->getUniqID());
            to_visit.push_back(child);
        }

        /// Only the child plans the step has already built, so that taking this snapshot cannot
        /// make a `ReadFromMerge` build one it would otherwise never have built.
        for (auto * child_plan : step.getBuiltChildPlans())
        {
            if (!child_plan || !child_plan->isInitialized())
                continue;
            auto * child_root = child_plan->getRootNode();
            if (!child_root || !child_root->step)
                continue;
            branch_node.children.push_back(child_root->step->getUniqID());
            to_visit.push_back(child_root);
        }

        if (const auto * join_step = typeid_cast<const JoinStep *>(&step))
        {
            branch_node.is_join = true;
            branch_node.cluster_id = join_step->getClusterId();
        }

        snapshot.join_nodes.push_back(std::move(branch_node));

        if (auto reporter = step.makeReporter())
            snapshot.reporters.emplace(step_id, std::move(reporter));

        snapshot.inputs.emplace(step_id, step.captureAnalysisInputs());
    }

    return snapshot;
}

}
