#pragma once

#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class GroupNode
{
public:
    GroupNode() = default;
    GroupNode(QueryPlanStepPtr step_) : step(step_) {}
    GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_) : step(step_), children(children_) {}

    void addChild(Group & group)
    {
        children.emplace_back(&group);
    }

    size_t childSize() const
    {
        return children.size();
    }

    const std::vector<Group *> getChildren() const
    {
        return children;
    }

    void replaceChildren(const std::vector<Group *> & children_)
    {
        children = children_;
    }

    QueryPlanStepPtr getStep() const
    {
        return step;
    }

private:
    QueryPlanStepPtr step;

    std::vector<Group *> children;

    std::unordered_map<PhysicalProperties, std::vector<PhysicalProperties>, PhysicalProperties::HashFunction> lowest_cost_expressions; /// output properties and input properties
};

}
