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
        GroupNodePtr group_node;
        Float64 cost;
    };

    Group() = default;

    Group(UInt32 id_);

    ~Group() = default;
    Group(Group &&) noexcept = default;
    Group & operator=(Group &&) noexcept = default;

    void addGroupNode(GroupNodePtr group_node, UInt32 group_node_id);

    GroupNodePtr getOneGroupNode();

    const std::list<GroupNodePtr> & getGroupNodes() const;

    std::list<GroupNodePtr> & getGroupNodes();

    void updatePropBestNode(const PhysicalProperties & properties, GroupNodePtr group_node, Float64 cost);

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

    std::list<GroupNodePtr> group_nodes;

    /// optimize temp result
    std::unordered_map<PhysicalProperties, GroupNodeCost, PhysicalProperties::HashFunction> prop_to_best_node;

    Statistics statistics;

    bool is_derive_stat = false;
};

}
