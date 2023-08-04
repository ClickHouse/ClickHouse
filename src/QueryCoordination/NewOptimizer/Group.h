#pragma once

#include <QueryCoordination/NewOptimizer/GroupPlanNode.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>

namespace DB
{

class Group
{
public:


private:
    std::vector<GroupPlanNode> group_expressions;

    std::unordered_map<PhysicalProperties, GroupPlanNode *, PhysicalProperties::HashFunction> lowest_cost_expressions;
};

}
