#pragma once

#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>

namespace DB
{

class Group
{
public:
    Group() = default;

    Group(const GroupNode & group_plan_node)
    {
        addGroupNode(group_plan_node);
    }

    const GroupNode & addGroupNode(const GroupNode & group_plan_node);

    GroupNode & getOneGroupNode()
    {
        return group_nodes[0];
    }

    const std::vector<GroupNode> & getGroupNodes() const
    {
        return group_nodes;
    }

    std::vector<GroupNode> & getGroupNodes()
    {
        return group_nodes;
    }

    void addLowestCostGroupNode(const PhysicalProperties & properties, const GroupNode * group_node, Float64 cost)
    {
        auto it = lowest_cost_expressions.find(properties);
        if (it == lowest_cost_expressions.end())
        {
            std::pair<Float64, const GroupNode *> cost_group_node{cost, group_node};
            lowest_cost_expressions.emplace(properties, cost_group_node);
        }
        else
        {
            if (cost < it->second.first)
            {
                std::pair<Float64, const GroupNode *> cost_group_node{cost, group_node};
                lowest_cost_expressions.emplace(properties, cost_group_node);
            }
        }
    }

    Float64 getCost(const PhysicalProperties & properties)
    {
        return lowest_cost_expressions[properties].first;
    }

    Float64 getLowestCost(const PhysicalProperties & required_properties)
    {
        Float64 min_cost = std::numeric_limits<Float64>::max();
        for (auto & [properties, cost_group_node] : lowest_cost_expressions)
        {
            if (properties.satisfy(required_properties))
            {
                if (cost_group_node.first < min_cost)
                {
                    min_cost = cost_group_node.first;
                }
            }
        }

        return min_cost;
    }

private:
    std::vector<GroupNode> group_nodes;

    std::unordered_map<PhysicalProperties, std::pair<Float64, const GroupNode *>, PhysicalProperties::HashFunction> lowest_cost_expressions;
};

}
