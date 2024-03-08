#pragma once

#include <Optimizer/Group.h>
#include <Optimizer/SubQueryPlan.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class Memo
{
public:
    Memo(QueryPlan && plan, ContextPtr context_);

    GroupNodePtr addPlanNodeToGroup(const QueryPlan::Node & node, Group * target_group);

    Group & buildGroup(const QueryPlan::Node & node);

    void dump();

    Group & rootGroup();

    QueryPlan extractPlan();
    SubQueryPlan extractPlan(Group & group, const PhysicalProperties & required_properties);

    UInt32 fetchAddGroupNodeId() { return ++group_node_id_counter; }

private:
    UInt32 group_id_counter{0};
    UInt32 group_node_id_counter{0};

    std::list<Group> groups;
    Group * root_group;

    std::unordered_set<GroupNodePtr, GroupNodeHash, GroupNodeEquals> all_group_nodes;

    ContextPtr context;
    Poco::Logger * log = &Poco::Logger::get("Memo");
};

}
