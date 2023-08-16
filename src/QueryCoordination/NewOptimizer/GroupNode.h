#pragma once

#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using AlternativeProperties = std::vector<std::vector<PhysicalProperties>>;

using OutPutPropAndAlternativeRequiredChildProp = std::unordered_map<PhysicalProperties, AlternativeProperties, PhysicalProperties::HashFunction>;

using OutPutPropAndRequiredChildProp = std::unordered_map<PhysicalProperties, std::vector<PhysicalProperties>, PhysicalProperties::HashFunction>;

class Group;

class GroupNode
{
public:
    GroupNode() = default;

    explicit GroupNode(QueryPlanStepPtr step_) : step(step_) {}
    GroupNode(QueryPlanStepPtr step_, bool is_enforce_node_) : step(step_), is_enforce_node(is_enforce_node_) {}

    GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_) : step(step_), children(children_) {}

    ~GroupNode() = default;
    GroupNode(GroupNode &&) noexcept = default;
    GroupNode & operator=(GroupNode &&) noexcept = default;

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

    void addLowestCostChildPropertyMap(const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & best_child_properties)
    {
        best_prop_map[physical_properties] = best_child_properties;
    }

    const std::vector<PhysicalProperties> & getChildProperties(const PhysicalProperties & physical_properties)
    {
        return best_prop_map[physical_properties];
    }

    OutPutPropAndRequiredChildProp & getOutPutPropAndRequiredChildProp()
    {
        return best_prop_map;
    }

    bool isEnforceNode() const
    {
        return is_enforce_node;
    }

    String toString() const;

    UInt32 getId() const
    {
        return id;
    }

    void setId(UInt32 id_)
    {
        id = id_;
    }

private:
    UInt32 id = 0;

    QueryPlanStepPtr step;

    bool is_enforce_node = false;

    std::vector<Group *> children;

    OutPutPropAndRequiredChildProp best_prop_map; /// output properties and input properties
};

}
