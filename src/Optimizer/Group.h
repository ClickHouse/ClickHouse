#pragma once

#include <Optimizer/Cost/Cost.h>
#include <Optimizer/GroupNode.h>
#include <Optimizer/PhysicalProperties.h>
#include <Optimizer/Statistics/Stats.h>

namespace DB
{

class Group
{
public:
    struct NodeAndCost
    {
        GroupNodePtr group_node;
        Cost cost;
    };

    Group() = default;
    Group(UInt32 id_);
    Group(Group &&) noexcept = default;

    ~Group() = default;
    Group & operator=(Group &&) noexcept = default;

    void addGroupNode(GroupNodePtr group_node, UInt32 group_node_id);
    GroupNodePtr getOneGroupNode();

    const std::vector<GroupNodePtr> & getGroupNodes() const;
    std::vector<GroupNodePtr> & getGroupNodes();

    bool updatePropBestNode(const PhysicalProperties & properties, GroupNodePtr group_node, Cost cost);

    Cost getCostByProp(const PhysicalProperties & properties);

    std::optional<std::pair<PhysicalProperties, Group::NodeAndCost>>
    getSatisfiedBestGroupNode(const PhysicalProperties & required_properties) const;

    UInt32 getId() const;

    String toString() const;
    String getDescription() const;

    void setStatistics(Stats & statistics_);
    const Stats & getStatistics() const;

    void setStatsDerived();
    bool hasStatsDerived() const;

private:
    UInt32 id = 0;
    std::vector<GroupNodePtr> group_nodes;

    /// optimize temp result
    std::unordered_map<PhysicalProperties, NodeAndCost, PhysicalProperties::HashFunction> prop_to_best;

    Stats statistics;
    bool stats_derived = false;
};

}
