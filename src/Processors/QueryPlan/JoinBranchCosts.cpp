#include <Processors/QueryPlan/JoinBranchCosts.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{

double cardinalityOf(const JoinStep * join_step, const CardinalityByJoinStep & cardinality_by_join_step)
{
    const auto it = cardinality_by_join_step.find(join_step);
    return it != cardinality_by_join_step.end() ? static_cast<double>(it->second) : 0.0;
}

bool sameCluster(const JoinStep * lhs, const JoinStep * rhs)
{
    return lhs->getClusterId() != 0 && lhs->getClusterId() == rhs->getClusterId();
}

}

JoinBranchCosts::JoinBranchCosts(const QueryPlan & plan, const CardinalityByJoinStep & cardinality_by_join_step)
{
    if (auto * root = plan.getRootNode())
        accumulate(root, cardinality_by_join_step);
}

std::vector<const JoinStep *> JoinBranchCosts::accumulate(QueryPlan::Node * node, const CardinalityByJoinStep & cardinality_by_join_step)
{
    std::vector<const JoinStep *> nearest_descendant_joins;
    for (auto * child : node->children)
    {
        auto below = accumulate(child, cardinality_by_join_step);
        nearest_descendant_joins.insert(nearest_descendant_joins.end(), below.begin(), below.end());
    }

    /// Embedded child plans are separate clusters: compute their costs but do not propagate them upward.
    for (auto * child_plan : node->step->getChildPlans())
        if (child_plan && child_plan->isInitialized())
            accumulate(child_plan->getRootNode(), cardinality_by_join_step);

    const auto * join_step = typeid_cast<const JoinStep *>(node->step.get());
    if (!join_step)
        return nearest_descendant_joins;

    double cost = cardinalityOf(join_step, cardinality_by_join_step);
    for (const auto * descendant : nearest_descendant_joins)
        if (sameCluster(descendant, join_step))
            cost += cost_by_join_step.at(descendant);

    cost_by_join_step[join_step] = cost;
    return {join_step};
}

std::optional<double> JoinBranchCosts::getBranchCost(const JoinStep * join_step) const
{
    if (const auto it = cost_by_join_step.find(join_step); it != cost_by_join_step.end())
        return it->second;
    return {};
}

}
