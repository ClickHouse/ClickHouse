#pragma once

#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class GroupPlanNode
{
public:
    GroupPlanNode(QueryPlanStepPtr step_, const std::vector<Group> & children_) : step(step_), children(children_) {}

private:
    QueryPlanStepPtr step;

    std::vector<Group> children;

    std::unordered_map<PhysicalProperties, std::vector<PhysicalProperties>, PhysicalProperties::HashFunction> lowest_cost_expressions; /// output properties and input properties
};

}
