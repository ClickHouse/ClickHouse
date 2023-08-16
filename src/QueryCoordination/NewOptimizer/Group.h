#pragma once

#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>

namespace DB
{

class Group
{
public:
    Group() = default;

    Group(GroupNode & group_plan_node, UInt32 id_) : id(id_)
    {
        addGroupNode(group_plan_node);
    }

    ~Group() = default;
    Group(Group &&) noexcept = default;
    Group & operator=(Group &&) noexcept = default;

    GroupNode & addGroupNode(GroupNode & group_plan_node);

    GroupNode & getOneGroupNode()
    {
        return group_nodes.front();
    }

    const std::list<GroupNode> & getGroupNodes() const
    {
        return group_nodes;
    }

    std::list<GroupNode> & getGroupNodes()
    {
        return group_nodes;
    }

    void addLowestCostGroupNode(const PhysicalProperties & properties, GroupNode * group_node, Float64 cost)
    {
        auto it = prop_to_best_node.find(properties);
        if (it == prop_to_best_node.end())
        {
            std::pair<Float64, GroupNode *> cost_group_node{cost, group_node};
            prop_to_best_node.emplace(properties, cost_group_node);
        }
        else
        {
            if (cost < it->second.first)
            {
                std::pair<Float64, GroupNode *> cost_group_node{cost, group_node};
                prop_to_best_node[properties] = cost_group_node;
            }
        }
    }

    Float64 getCost(const PhysicalProperties & properties)
    {
        return prop_to_best_node[properties].first;
    }

    std::pair<GroupNode *, PhysicalProperties> getBestGroupNode(const PhysicalProperties & required_properties) const
    {
        Float64 min_cost = std::numeric_limits<Float64>::max();

        std::pair<GroupNode *, PhysicalProperties> res{nullptr, {}};

        for (auto & [properties, cost_group_node] : prop_to_best_node)
        {
            if (properties.satisfy(required_properties))
            {
                if (cost_group_node.first < min_cost)
                {
                    min_cost = cost_group_node.first;
                    res.first = cost_group_node.second;
                    res.second = properties;
                }
            }
        }
        if (!res.first)
            throw;

        return res;
    }

    Float64 getLowestCost(const PhysicalProperties & required_properties) const
    {
        Float64 min_cost = std::numeric_limits<Float64>::max();
        for (auto & [properties, cost_group_node] : prop_to_best_node)
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

    UInt32 getId() const
    {
        return id;
    }

    String toString() const
    {
        String res;
        res += std::to_string(getId()) + ",";

        res += "group_nodes: ";
        for (auto & node : group_nodes)
        {
            res += "{ " + node.toString() + "}, ";
        }

        String prop_map;
        for(auto & [prop, cost_group_node] : prop_to_best_node)
        {
            prop_map += "{ " + prop.toString() + "-" + std::to_string(cost_group_node.first) + "|" + std::to_string(cost_group_node.second->getId()) + "}, ";
        }

        res += "prop_to_best_node: " + prop_map;
        return res;
    }

private:
    UInt32 id = 0;

    std::list<GroupNode> group_nodes;

    std::unordered_map<PhysicalProperties, std::pair<Float64, GroupNode *>, PhysicalProperties::HashFunction> prop_to_best_node;
};

}
