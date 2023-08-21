#pragma once

#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class Memo
{
public:
    Memo(QueryPlan && plan, ContextPtr context_);

    void addPlanNodeToGroup(const QueryPlan::Node & node, Group & target_group);

    Group & buildGroup(const QueryPlan::Node & node);

    Group & buildGroup(const QueryPlan::Node & node, const std::vector<Group *> children_groups);

    void dump(Group & group);

    void transform();

    void transform(Group & group, std::unordered_map<Group *, std::vector<SubQueryPlan>> & group_transformed_node);

    void enforce();

    std::optional<std::pair<PhysicalProperties, Group::GroupNodeCost>> enforce(Group & group, const PhysicalProperties & required_properties);

    void derivationProperties();

    void derivationProperties(Group * group);

    SubQueryPlan extractPlan();

    SubQueryPlan extractPlan(Group & group, const PhysicalProperties & required_properties);

private:
    void enforceGroupNode(
        const PhysicalProperties & required_prop,
        const PhysicalProperties & output_prop,
        GroupNode & group_node,
        std::vector<std::pair<GroupNode, PhysicalProperties>> & collection);

    UInt32 group_id_counter = 0;

    UInt32 group_node_id_counter = 0;

    std::list<Group> groups;

    Group * root_group;

    ContextPtr context;

    Poco::Logger * log = &Poco::Logger::get("Memo");
};

}
