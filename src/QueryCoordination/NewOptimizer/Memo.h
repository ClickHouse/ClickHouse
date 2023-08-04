#pragma once

#include <QueryCoordination/NewOptimizer/Group.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class Memo
{
public:
    explicit Memo(QueryPlan && plan)
    {

    }

    void addPlanNode(const QueryPlan::Node & node, Group & target_group)
    {
//        node.step
    }

private:
    std::vector<Group> groups;

    Group root_group;
};

}
