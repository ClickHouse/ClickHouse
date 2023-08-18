#pragma once

#include <QueryCoordination/NewOptimizer/GroupNode.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>

namespace DB
{

class Group
{
public:
    struct GroupNodeCost
    {
        GroupNode * group_node = nullptr;
        Float64 cost;
    };

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

    void updatePropBestNode(const PhysicalProperties & properties, GroupNode * group_node, Float64 cost);

    Float64 getCostByProp(const PhysicalProperties & properties);

    std::pair<PhysicalProperties, GroupNodeCost> getSatisfyBestGroupNode(const PhysicalProperties & required_properties) const;

    Float64 getSatisfyBestCost(const PhysicalProperties & required_properties) const;

    UInt32 getId() const
    {
        return id;
    }

    String toString() const;

private:
    UInt32 id = 0;

    std::list<GroupNode> group_nodes;

    std::unordered_map<PhysicalProperties, GroupNodeCost, PhysicalProperties::HashFunction> prop_to_best_node;
};

}
