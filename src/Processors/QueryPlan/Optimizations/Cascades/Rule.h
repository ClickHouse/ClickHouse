#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Memo;

/// A Full sort with a limit is a top-N: it reduces rows, so it stays in the memo as an operator
/// (a limit-less Full sort is stripped into a sorting property). Only a Full sort takes unsorted
/// input; FinishSorting/MergingSorted need ordered input, which no rule provides.
bool isTopNSort(const IQueryPlanStep & step);

/// Stateless per-row steps that can run on any data partition independently. Implemented by
/// `DistributionPassthrough` and therefore excluded from `DefaultImplementation` - both go
/// through this one predicate so a new passthrough step type cannot end up with two
/// implementation rules or none.
bool isDistributionPassthroughStep(const IQueryPlanStep & step);

/// Builds the self-referential expression an enforcer inserts: it lives in the SOURCE
/// expression's group and its single input points back to the same group with relaxed
/// requirements, so the memo search recurses into the group to satisfy them. The axis marks
/// the expression for the cycle-avoidance rules. Only constructs - the caller still calls
/// `addPhysicalToMemo` with the original required properties.
GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    EnforcerAxis axis);

class IOptimizationRule
{
public:
    virtual ~IOptimizationRule() = default;
    virtual String getName() const = 0;
    virtual bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const = 0;
    virtual Promise getPromise() const = 0;
    virtual bool isTransformation() const = 0;

    std::vector<GroupExpressionPtr> apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const;

protected:
    virtual std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const = 0;

    /// Inserts a rule-produced physical expression into its group and appends it to `result` when
    /// it was actually inserted (a structural duplicate is dropped). Marks the expression with
    /// this rule so it is not re-applied to its own product.
    void addPhysicalToMemo(GroupExpressionPtr expression, const ExpressionProperties & required_properties,
        Memo & memo, std::vector<GroupExpressionPtr> & result) const;
};

/// Clones a plan step and returns it as its concrete type; throws if the clone has another type.
template <typename Step>
std::unique_ptr<Step> cloneStepAs(const Step & step)
{
    auto clone = step.clone();
    if (typeid_cast<Step *>(clone.get()) == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Clone of '{}' has unexpected type", step.getName());
    return std::unique_ptr<Step>(static_cast<Step *>(clone.release()));
}

using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

}
