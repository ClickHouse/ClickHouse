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

    void addGroupNode(const GroupNode & group_plan_node);

    GroupNode & getOneGroupNode()
    {
        return group_nodes[0];
    }

    const std::vector<GroupNode> & getGroupNodes() const
    {
        return group_nodes;
    }

private:
    std::vector<GroupNode> group_nodes;

    std::unordered_map<PhysicalProperties, GroupNode *, PhysicalProperties::HashFunction> lowest_cost_expressions;
};

}
