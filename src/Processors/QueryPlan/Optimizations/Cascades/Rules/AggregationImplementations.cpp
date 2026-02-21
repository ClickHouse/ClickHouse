#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Produces all applicable single-phase aggregation implementations:
///   - Local: gather all data to one node, aggregate there (always applicable)
///   - Shuffle: input pre-distributed by group keys, each node aggregates its own key partition
///     (only applicable when node_count > 1 and there are GROUP BY keys)
///
/// Two-phase (partial + shuffle + merge) aggregation is handled separately by
/// TwoPhaseAggregationTransformation, which splits a logical Agg into
/// FinalAgg(PartialAgg(input)) before implementations are assigned.
class AggregationImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "Aggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// Logical transformation that splits a single-phase Agg into a two-phase plan:
///   PartialAgg(input) → ShuffleExchange → FinalMergeAgg
///
/// PartialAgg computes partial aggregate states locally on each node without finalization.
/// FinalMergeAgg receives pre-aggregated states and merges them into final results.
/// The exchange between the two is inserted by the DistributionEnforcer based on the
/// distribution requirements set by the implementation rules on FinalMergeAgg.
///
/// This split is only attempted for aggregations that support it (canUseProjection).
class TwoPhaseAggregationTransformation : public IOptimizationRule
{
public:
    String getName() const override { return "TwoPhaseAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};


bool AggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        agg_step->getFinal();   /// applies to both original Agg and FinalMergeAgg from two-phase split
}

std::vector<GroupExpressionPtr> AggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    chassert(agg_step);
    chassert(expression->inputs.size() == 1);

    const size_t cluster_node_count = 4;    /// TODO: get actual cluster topology

    /// When the parent explicitly requires multi-node distribution, match that count.
    /// Otherwise fall back to the cluster size.
    const size_t distributed_node_count = required_properties.distribution.node_count > 1
        ? required_properties.distribution.node_count
        : cluster_node_count;

    std::vector<GroupExpressionPtr> result;

    /// Strategy A: Local — gather all input to one node, aggregate there.
    /// Always applicable; when distributed_node_count == 1 it is also the only meaningful strategy.
    {
        auto new_step = agg_step->clone();
        new_step->setStepDescription(fmt::format("Local IMPL: {}", agg_step->getStepDescription()), 200);

        GroupExpressionPtr local_agg = std::make_shared<GroupExpression>(*expression);
        local_agg->plan_step = std::move(new_step);

        DistributionDescription single_node;    /// node_count=1 (default)
        local_agg->inputs[0].required_properties.distribution = single_node;
        local_agg->properties.distribution = single_node;

        local_agg->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(local_agg);
        result.push_back(local_agg);
    }

    /// For a single-node cluster distributed strategies are identical to local — skip them.
    if (distributed_node_count == 1)
        return result;

    /// Strategy B: Shuffle — input pre-distributed by group keys, each node aggregates its
    /// own partition of keys and produces a final result independently.
    /// Not applicable for global aggregations (e.g. COUNT(*)) that have no group keys.
    if (!agg_step->getParams().keys.empty())
    {
        auto new_step = agg_step->clone();
        new_step->setStepDescription(fmt::format("Shuffle IMPL: {}", agg_step->getStepDescription()), 200);

        DistributionDescription by_keys;
        by_keys.node_count = distributed_node_count;
        for (const auto & key : agg_step->getParams().keys)
            by_keys.columns.push_back({key});

        GroupExpressionPtr shuffle_agg = std::make_shared<GroupExpression>(*expression);
        shuffle_agg->plan_step = std::move(new_step);
        shuffle_agg->inputs[0].required_properties.distribution = by_keys;
        shuffle_agg->properties.distribution = by_keys;

        shuffle_agg->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(shuffle_agg);
        result.push_back(shuffle_agg);
    }

    return result;
}

OptimizationRulePtr createAggregationImplementation() { return std::make_shared<AggregationImplementation>(); }


bool TwoPhaseAggregationTransformation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        !expression->getQueryPlanStep()->getStepDescription().contains("Partial:") &&
        agg_step->getFinal() &&
        !agg_step->getParams().only_merge &&    /// don't split a merge step that's already from a prior split
        agg_step->canUseProjection();           /// needed for requestOnlyMergeForAggregateProjection
}

std::vector<GroupExpressionPtr> TwoPhaseAggregationTransformation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    chassert(agg_step);
    chassert(expression->inputs.size() == 1);

    /// Phase 1: partial aggregation — takes raw rows, outputs intermediate aggregate states.
    auto partial_step_ptr = agg_step->clone();
    auto * partial_step = dynamic_cast<AggregatingStep *>(partial_step_ptr.get());
    chassert(partial_step);
    partial_step->setFinal(false);
    partial_step->setStepDescription(fmt::format("Partial: {}", agg_step->getStepDescription()), 200);

    /// Phase 2: merge aggregation — takes aggregate states from Phase 1, produces final results.
    /// requestOnlyMergeForAggregateProjection sets only_merge=true and adapts the input header.
    auto merge_step_ptr = agg_step->clone();
    auto * merge_step = dynamic_cast<AggregatingStep *>(merge_step_ptr.get());
    chassert(merge_step);
    merge_step->requestOnlyMergeForAggregateProjection(partial_step->getOutputHeader());
    merge_step->setStepDescription(fmt::format("Merge: {}", agg_step->getStepDescription()), 200);

    /// Create a new group for the partial aggregation, referencing the original inputs.
    GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(std::move(partial_step_ptr));
    partial_expr->inputs = expression->inputs;
    GroupId partial_group_id = memo.addGroup(partial_expr);

    /// Add the merge aggregation as a logical alternative in the original group.
    /// Its implementation rules will set the distribution requirements (Local or Shuffle),
    /// causing the DistributionEnforcer to insert the appropriate exchange before the partial step.
    GroupExpressionPtr merge_expr = std::make_shared<GroupExpression>(std::move(merge_step_ptr));
    merge_expr->inputs = {{partial_group_id, {}}};
    merge_expr->setApplied(*this, {});
    memo.getGroup(expression->group_id)->addLogicalExpression(merge_expr);

    return {merge_expr};
}

OptimizationRulePtr createTwoPhaseAggregationTransformation() { return std::make_shared<TwoPhaseAggregationTransformation>(); }

}
