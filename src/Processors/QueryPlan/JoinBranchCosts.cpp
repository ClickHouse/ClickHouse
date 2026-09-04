#include <algorithm>
#include <optional>
#include <vector>
#include <Processors/QueryPlan/JoinBranchCosts.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/typeid_cast.h>
#include <base/defines.h>

namespace DB
{

namespace
{

bool sameCluster(const JoinStep * lhs, const JoinStep * rhs)
{
    return lhs->getClusterId() != 0 && lhs->getClusterId() == rhs->getClusterId();
}

std::vector<const QueryPlan::Node *> collectPostOrder(const QueryPlan::Node * root)
{
    std::vector<const QueryPlan::Node *> post_order;
    std::vector<const QueryPlan::Node *> to_visit{root};

    while (!to_visit.empty())
    {
        const auto * node = to_visit.back();
        to_visit.pop_back();
        post_order.push_back(node);

        for (const auto * child : node->children)
            to_visit.push_back(child);

        for (const auto * child_plan : node->step->getChildPlans())
            if (child_plan && child_plan->isInitialized())
                to_visit.push_back(child_plan->getRootNode());
    }

    std::reverse(post_order.begin(), post_order.end());
    return post_order;
}

}

JoinBranchCosts::JoinBranchCosts(const QueryPlan & plan, const CardinalityByJoinStep & cardinality_by_join_step)
{
    if (const auto * root = plan.getRootNode())
        accumulate(root, cardinality_by_join_step);
}

void JoinBranchCosts::accumulate(const QueryPlan::Node * root, const CardinalityByJoinStep & cardinality_by_join_step)
{
    std::unordered_map<const QueryPlan::Node *, std::vector<const JoinStep *>> joins_below;

    for (const auto * node : collectPostOrder(root))
    {
        /// collect the joins from children
        std::vector<const JoinStep *> joins_below_the_curren_node;
        for (const auto * child : node->children)
        {
            const auto it = joins_below.find(child);
            chassert(it != joins_below.end(), "post-order traversal must have processed the child already");
            joins_below_the_curren_node.insert(joins_below_the_curren_node.end(), it->second.begin(), it->second.end());
            joins_below.erase(it);
        }

        const auto * join_step = typeid_cast<const JoinStep *>(node->step.get());

        /// If step is not join -- propagate the joins below it
        if (!join_step)
        {
            joins_below[node] = std::move(joins_below_the_curren_node);
            continue;
        }

        std::optional<UInt64> cost;
        if (const auto it = cardinality_by_join_step.find(join_step); it != cardinality_by_join_step.end())
            cost = it->second;

        /// For every join in the same cluster, add its cost to the cost of this join
        for (const auto * descendant : joins_below_the_curren_node)
        {
            if (!sameCluster(descendant, join_step))
                continue;

            const auto & descendant_cost = cost_by_join_step.at(descendant);
            if (cost && descendant_cost)
                *cost += *descendant_cost;
            else
                cost = std::nullopt;
        }

        cost_by_join_step[join_step] = cost;
        joins_below[node] = {join_step};
    }
}

std::optional<UInt64> JoinBranchCosts::getBranchCost(const JoinStep * join_step) const
{
    if (const auto it = cost_by_join_step.find(join_step); it != cost_by_join_step.end())
        return it->second;
    return {};
}

}
