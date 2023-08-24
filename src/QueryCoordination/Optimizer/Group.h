#pragma once

#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

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

    Group(GroupNode & group_plan_node, UInt32 id_);

    ~Group() = default;
    Group(Group &&) noexcept = default;
    Group & operator=(Group &&) noexcept = default;

    GroupNode & addGroupNode(GroupNode & group_plan_node);

    GroupNode & getOneGroupNode();

    const std::list<GroupNode> & getGroupNodes() const;

    std::list<GroupNode> & getGroupNodes();

    void updatePropBestNode(const PhysicalProperties & properties, GroupNode * group_node, Float64 cost);

    Float64 getCostByProp(const PhysicalProperties & properties);

    std::optional<std::pair<PhysicalProperties, Group::GroupNodeCost>> getSatisfyBestGroupNode(const PhysicalProperties & required_properties) const;

    Float64 getSatisfyBestCost(const PhysicalProperties & required_properties) const;

    UInt32 getId() const;

    String toString() const;

    void setStatistics(Statistics & statistics_);

    const Statistics & getStatistics() const;

    void setDeriveStat();

    bool isDeriveStat() const;

private:
    UInt32 id = 0;

    std::list<GroupNode> group_nodes;

    /// optimize temp result
    std::unordered_map<PhysicalProperties, GroupNodeCost, PhysicalProperties::HashFunction> prop_to_best_node;

    Statistics statistics;

    bool is_derive_stat = false;
};

}
