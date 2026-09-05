#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <memory>

namespace DB
{

class Memo;

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

    /// Same, for a rule that produces one expression: returns the result list directly.
    std::vector<GroupExpressionPtr> addPhysicalToMemo(
        GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const;

    /// Registers a two-stage split of `source_expression`: the partial expression becomes its own
    /// group (inheriting the source's inputs) and `final_step` becomes a logical alternative in
    /// the source group, consuming the partial group with the given input requirements. Returns
    /// the final expression, marked with this rule so the split is not re-applied to it.
    GroupExpressionPtr addTwoStageSplit(Memo & memo, const GroupExpressionPtr & source_expression,
        GroupExpressionPtr partial_expression, QueryPlanStepPtr final_step,
        ExpressionProperties final_input_required) const;
};

using OptimizationRulePtr = std::shared_ptr<const IOptimizationRule>;

/// Rule factories; every rule class is local to its source file under `Rules/`.
OptimizationRulePtr createJoinCommutativity();
OptimizationRulePtr createHashJoinImplementation();
OptimizationRulePtr createAggregationImplementation();
OptimizationRulePtr createTwoStageAggregationTransformation();
OptimizationRulePtr createAggregationPushdown();
OptimizationRulePtr createLocalReadImplementation();
OptimizationRulePtr createParallelReadImplementation();
OptimizationRulePtr createReplicatedReadImplementation();
OptimizationRulePtr createReplicatedSubplanImplementation();
OptimizationRulePtr createTopNImplementation();
OptimizationRulePtr createTwoStageTopN();
OptimizationRulePtr createDefaultImplementation();
OptimizationRulePtr createDistributionPassthrough();
OptimizationRulePtr createDistributionEnforcer();
OptimizationRulePtr createSortingEnforcer();

}
