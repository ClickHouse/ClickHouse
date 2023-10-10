#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>

namespace DB
{

using ChildrenProp = std::vector<PhysicalProperties>;

using AlternativeChildrenProp = std::vector<ChildrenProp>;

using PropAndAlternativeChildrenProp = std::unordered_map<PhysicalProperties, AlternativeChildrenProp, PhysicalProperties::HashFunction>;

class Group;

class GroupNode final : public std::enable_shared_from_this<GroupNode>
{
public:
    struct ChildrenPropCost
    {
        std::vector<PhysicalProperties> child_prop;
        Float64 cost;
    };

    using PropAndChildrenProp = std::unordered_map<PhysicalProperties, ChildrenPropCost, PhysicalProperties::HashFunction>;

    GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_, bool is_enforce_node_ = false);

    ~GroupNode();

    GroupNode(GroupNode &&) noexcept;

    void addChild(Group & child);

    size_t childSize() const;

    std::vector<Group *> getChildren() const;

    QueryPlanStepPtr getStep() const;

    Group & getGroup();

    void setGroup(Group * group_);

    void updateBestChild(const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & child_properties, Float64 child_cost);

    const std::vector<PhysicalProperties> & getChildrenProp(const PhysicalProperties & physical_properties);

    void addRequiredChildrenProp(ChildrenProp & child_pro);

    AlternativeChildrenProp & getRequiredChildrenProp();

    bool hasRequiredChildrenProp() const;

    bool isEnforceNode() const;

    String toString() const;

    UInt32 getId() const;

    void setId(UInt32 id_);

    void setDerivedStat();

    bool isDerivedStat() const;

    template <class Visitor>
    typename Visitor::ResultType accept(Visitor & visitor)
    {
        return visitor.visit(step);
    }

private:
    QueryPlanStepPtr step;

    UInt32 id;

    Group * group;

    std::vector<Group *> children;

    bool is_enforce_node;

    /// output properties and input properties
    PropAndChildrenProp prop_to_best_child;

    AlternativeChildrenProp required_children_prop;

    bool is_derived_stat;
};

using GroupNodePtr = std::shared_ptr<GroupNode>;

struct GroupNodeHash
{
    std::size_t operator()(const GroupNodePtr & s) const
    {
        if (!s)
            return 0;

        size_t hash = 0;
//        size_t hash = s->getStep()->hash();
//        hash = MurmurHash3Impl64::combineHashes(hash, IntHash64Impl::apply(child_groups.size()));
//        for (auto child_group : child_groups)
//        {
//            hash = MurmurHash3Impl64::combineHashes(hash, child_group);
//        }
        return hash;
    }
};

struct GroupNodeEquals
{
    bool operator()(const GroupNodePtr & /*t1*/, const GroupNodePtr & /*t2*/) const { return false; }
};

}
