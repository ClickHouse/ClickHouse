#include <Processors/QueryPlan/Profiling/Analysis/JoinBranchCosts.h>

#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace
{

bool sameCluster(const StepAnalyzer & lhs, const StepAnalyzer & rhs)
{
    return lhs.cluster_id != 0 && lhs.cluster_id == rhs.cluster_id;
}

}

JoinBranchCosts::JoinBranchCosts(const PlanAnalysisSnapshot & plan_snapshot, const CardinalityByJoinStep & cardinality_by_join_step)
{
    /// The joins of each node's subtree, carried up as the post-order walk retires its children.
    std::unordered_map<String, std::vector<const StepAnalyzer *>> joins_below;

    for (const auto * node : plan_snapshot.stepsInPostOrder())
    {
        /// collect the joins from children
        std::vector<const StepAnalyzer *> joins_below_the_current_node;
        for (const auto & child_id : node->children)
        {
            const auto it = joins_below.find(child_id);
            if (it == joins_below.end())
                continue;
            joins_below_the_current_node.insert(joins_below_the_current_node.end(), it->second.begin(), it->second.end());
            joins_below.erase(it);
        }

        /// If step is not join -- propagate the joins below it
        if (!node->is_join)
        {
            joins_below[node->id] = std::move(joins_below_the_current_node);
            continue;
        }

        std::optional<UInt64> cost;
        if (const auto it = cardinality_by_join_step.find(node->id); it != cardinality_by_join_step.end())
            cost = it->second;

        /// For every join in the same cluster, add its cost to the cost of this join
        for (const auto * descendant : joins_below_the_current_node)
        {
            if (!sameCluster(*descendant, *node))
                continue;

            const auto & descendant_cost = cost_by_join_step.at(descendant->id);
            if (cost && descendant_cost)
                *cost += *descendant_cost;
            else
                cost = std::nullopt;
        }

        cost_by_join_step[node->id] = cost;
        joins_below[node->id] = {node};
    }
}

std::optional<UInt64> JoinBranchCosts::getBranchCost(const String & step_id) const
{
    if (const auto it = cost_by_join_step.find(step_id); it != cost_by_join_step.end())
        return it->second;
    return {};
}

}
