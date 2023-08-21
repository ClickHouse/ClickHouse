#pragma once

#include <QueryCoordination/NewOptimizer/PhysicalProperties.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using ChildrenProp = std::vector<PhysicalProperties>;

using AlternativeChildrenProp = std::vector<ChildrenProp>;

using PropAndAlternativeChildrenProp = std::unordered_map<PhysicalProperties, AlternativeChildrenProp, PhysicalProperties::HashFunction>;

class Group;

class GroupNode
{
public:
    struct ChildrenPropCost
    {
        std::vector<PhysicalProperties> child_prop;
        Float64 cost;
    };

    using PropAndChildrenProp = std::unordered_map<PhysicalProperties, ChildrenPropCost, PhysicalProperties::HashFunction>;

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

    std::vector<Group *> getChildren() const
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

    void updateBestChild(const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & child_properties, Float64 child_cost);

    const std::vector<PhysicalProperties> & getChildrenProp(const PhysicalProperties & physical_properties);

    void addRequiredChildrenProp(ChildrenProp & child_pro)
    {
        required_children_prop.emplace_back(child_pro);
    }

    AlternativeChildrenProp & getRequiredChildrenProp()
    {
        return required_children_prop;
    }

    bool hasRequiredChildrenProp() const
    {
        return !required_children_prop.empty();
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

    /// output properties and input properties
    PropAndChildrenProp prop_to_best_child;

    AlternativeChildrenProp required_children_prop;
};

}
