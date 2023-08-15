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

    Float64 enforce(Group & group, const PhysicalProperties & required_properties);

    void derivationProperties();

    void derivationProperties(Group * group);

    QueryPlan extractPlan();

    QueryPlan extractPlan(Group & group, const PhysicalProperties & required_properties);

private:
    std::vector<Group> groups;

    Group * root_group;

    ContextPtr context;
};

}
