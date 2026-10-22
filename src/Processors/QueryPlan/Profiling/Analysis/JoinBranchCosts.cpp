#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Processors/QueryPlan/Profiling/Analysis/JoinBranchCosts.h>
#include <base/defines.h>

namespace DB
{

namespace
{

bool sameCluster(const PlanShapeNode & lhs, const PlanShapeNode & rhs)
{
    return lhs.cluster_id != 0 && lhs.cluster_id == rhs.cluster_id;
}

using NodesById = std::unordered_map<String, const PlanShapeNode *>;

std::vector<const PlanShapeNode *> collectPostOrder(const NodesById & nodes_by_id, const String & root_id)
{
    const auto root_it = nodes_by_id.find(root_id);
    if (root_it == nodes_by_id.end())
        return {};

    std::vector<const PlanShapeNode *> post_order;
    std::vector<const PlanShapeNode *> to_visit{root_it->second};

    while (!to_visit.empty())
    {
        const auto * node = to_visit.back();
        to_visit.pop_back();
        post_order.push_back(node);

        for (const auto & child_id : node->children)
            if (const auto it = nodes_by_id.find(child_id); it != nodes_by_id.end())
                to_visit.push_back(it->second);
    }

    std::reverse(post_order.begin(), post_order.end());
    return post_order;
}

}

JoinBranchCosts::JoinBranchCosts(
    const PlanShape & nodes, const String & root_id, const CardinalityByJoinStep & cardinality_by_join_step)
{
    NodesById nodes_by_id;
    nodes_by_id.reserve(nodes.size());
    for (const auto & node : nodes)
        nodes_by_id.emplace(node.id, &node);

    /// The joins of each node's subtree, carried up as the post-order walk retires its children.
    std::unordered_map<String, std::vector<const PlanShapeNode *>> joins_below;

    for (const auto * node : collectPostOrder(nodes_by_id, root_id))
    {
        /// collect the joins from children
        std::vector<const PlanShapeNode *> joins_below_the_current_node;
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
