#pragma once

#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using AlternativeProperties = std::vector<std::vector<PhysicalProperties>>;

using OutPutPropAndAlternativeRequiredChildProp = std::unordered_map<PhysicalProperties, AlternativeProperties, PhysicalProperties::HashFunction>;

using OutPutPropAndRequiredChildProp = std::unordered_map<PhysicalProperties, std::vector<PhysicalProperties>, PhysicalProperties::HashFunction>;

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

    void addOutPutProperties(const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & best_child_properties)
    {
        lowest_cost_expressions[physical_properties] = best_child_properties;
    }

    OutPutPropAndRequiredChildProp & getOutPutPropAndRequiredChildProp()
    {
        return lowest_cost_expressions;
    }

private:
    QueryPlanStepPtr step;

    std::vector<Group *> children;

    OutPutPropAndRequiredChildProp lowest_cost_expressions; /// output properties and input properties
};

}
