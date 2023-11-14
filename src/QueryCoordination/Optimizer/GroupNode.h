#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/Optimizer/Cost/Cost.h>
#include <QueryCoordination/Optimizer/PhysicalProperties.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>
#include <QueryCoordination/Optimizer/Rule/RuleSet.h>

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
        Cost cost;
    };

    using PropAndChildrenProp = std::unordered_map<PhysicalProperties, ChildrenPropCost, PhysicalProperties::HashFunction>;

    GroupNode(GroupNode &&) noexcept;
    GroupNode(QueryPlanStepPtr step_, const std::vector<Group *> & children_, bool is_enforce_node_ = false);

    ~GroupNode();

    void addChild(Group & child);
    size_t childSize() const;
    std::vector<Group *> getChildren() const;

    QueryPlanStepPtr getStep() const;

    Group & getGroup();
    void setGroup(Group * group_);

    bool updateBestChild(
        const PhysicalProperties & physical_properties, const std::vector<PhysicalProperties> & child_properties, Cost child_cost);

    const std::vector<PhysicalProperties> & getChildrenProp(const PhysicalProperties & physical_properties);

    void addRequiredChildrenProp(ChildrenProp & child_pro);

    AlternativeChildrenProp & getRequiredChildrenProp();
    bool hasRequiredChildrenProp() const;

    bool isEnforceNode() const;

    String toString() const;
    String getDescription() const;

    UInt32 getId() const;
    void setId(UInt32 id_);

    void setStatsDerived();
    bool hasStatsDerived() const;

    bool hasApplied(size_t rule_id) const;

    void setApplied(size_t rule_id);

    template <class Visitor>
    typename Visitor::ResultType accept(Visitor & visitor)
    {
        return visitor.visit(step);
    }

private:
    UInt32 id;
    QueryPlanStepPtr step;

    Group * group;
    std::vector<Group *> children;

    bool is_enforce_node;

    /// output properties and input properties
    PropAndChildrenProp prop_to_best_child;
    AlternativeChildrenProp required_children_prop;

    bool stats_derived;

    std::bitset<CostBasedOptimizer::RULES_SIZE> rule_masks;
};

using GroupNodePtr = std::shared_ptr<GroupNode>;

struct GroupNodeHash
{
    std::size_t operator()(const GroupNodePtr & s) const
    {
        if (!s)
            return 0;

        size_t hash = 0;
        /// TODO implement
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
    /// TODO implement
    bool operator()(const GroupNodePtr & /*lhs*/, const GroupNodePtr & /*rhs*/) const { return false; }
};

}
