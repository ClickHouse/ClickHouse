#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
        expression->strategy == nullptr;
}

std::vector<GroupExpressionPtr> AggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationImplementation::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationImplementation::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    const size_t cluster_node_count = memo.getClusterNodeCount();
    const auto candidate_node_counts = getCandidateNodeCounts(cluster_node_count);

    /// Partial (non-final) aggregation: create distributed implementations at each candidate
    /// node count. The output distribution is `{node_count, []}` (no column guarantee).
    /// When the parent (MergingAggregated) requires `{1 node}`, the DistributionEnforcer
    /// bridges the gap via GatherExchange — crucially on the PARTIAL output (~25 rows) rather
    /// than the raw input (~1M rows). This produces:
    ///   ParallelRead → Expression → PartialAgg({N nodes}) → GatherExchange → MergeAgg
    /// We intentionally do NOT create a `{1 node}` variant here: if one existed, it would
    /// become the best for `{1 node}` immediately, preventing the enforcer from ever running
    /// and producing the cheaper GatherExchange-on-partial-output plan.
    if (!agg_step->getFinal())
    {
        std::vector<GroupExpressionPtr> result;
        for (size_t candidate_node_count : candidate_node_counts)
        {
            auto new_step = agg_step->clone();
            new_step->setStepDescription(*agg_step);

            GroupExpressionPtr partial_impl = std::make_shared<GroupExpression>(*expression);
            partial_impl->plan_step = std::move(new_step);
            partial_impl->strategy = std::make_shared<PartialAggregationStrategy>();

            DistributionDescription dist;
            dist.node_count = candidate_node_count;
            partial_impl->inputs[0].required_properties.distribution = dist;
            partial_impl->properties.distribution = dist;

            partial_impl->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(partial_impl);
            result.push_back(partial_impl);
        }
        return result;
    }

    std::vector<GroupExpressionPtr> result;

    /// Strategy A: Local — gather all input to one node, aggregate there.
    /// Always applicable; when the cluster has only 1 node it is also the only meaningful strategy.
    {
        auto new_step = agg_step->clone();
        new_step->setStepDescription(fmt::format("Local {}", agg_step->getStepDescription()), 200);

        GroupExpressionPtr local_agg = std::make_shared<GroupExpression>(*expression);
        local_agg->plan_step = std::move(new_step);
        local_agg->strategy = std::make_shared<LocalAggregationStrategy>();

        DistributionDescription single_node;    /// node_count=1 (default)
        local_agg->inputs[0].required_properties.distribution = single_node;
        local_agg->properties.distribution = single_node;

        local_agg->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(local_agg);
        result.push_back(local_agg);
    }

    /// For a single-node cluster distributed strategies are identical to local — skip them.
    if (candidate_node_counts.empty())
        return result;

    /// Strategy B: Shuffle — input pre-distributed by group keys, each node aggregates its
    /// own partition of keys and produces a final result independently.
    /// Not applicable for global aggregations (e.g. COUNT(*)) that have no group keys.
    if (!agg_step->getParams().keys.empty())
    {
        for (size_t candidate_node_count : candidate_node_counts)
        {
            auto new_step = agg_step->clone();
            new_step->setStepDescription(fmt::format("Shuffle {}", agg_step->getStepDescription()), 200);

            DistributionDescription by_keys;
            by_keys.node_count = candidate_node_count;
            for (const auto & key : agg_step->getParams().keys)
                by_keys.columns.push_back({key});

            GroupExpressionPtr shuffle_agg = std::make_shared<GroupExpression>(*expression);
            shuffle_agg->plan_step = std::move(new_step);
            shuffle_agg->strategy = std::make_shared<ShuffleAggregationStrategy>();
            shuffle_agg->inputs[0].required_properties.distribution = by_keys;
            shuffle_agg->properties.distribution = by_keys;

            shuffle_agg->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(shuffle_agg);
            result.push_back(shuffle_agg);
        }
    }

    return result;
}

OptimizationRulePtr createAggregationImplementation() { return std::make_shared<AggregationImplementation>(); }


bool TwoPhaseAggregationTransformation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step != nullptr &&
        expression->strategy == nullptr &&
        agg_step->getFinal() &&
        !agg_step->getParams().only_merge;       /// don't split a merge step that's already from a prior split
}

std::vector<GroupExpressionPtr> TwoPhaseAggregationTransformation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoPhaseAggregationTransformation::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoPhaseAggregationTransformation::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    /// Phase 1: partial aggregation — takes raw rows, outputs intermediate aggregate states.
    auto partial_step_ptr = agg_step->clone();
    auto * partial_step = dynamic_cast<AggregatingStep *>(partial_step_ptr.get());
    if (!partial_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoPhaseAggregationTransformation: clone of AggregatingStep '{}' did not produce an AggregatingStep",
            agg_step->getStepDescription());
    partial_step->setFinal(false);
    partial_step->setStepDescription(fmt::format("Partial: {}", agg_step->getStepDescription()), 200);

    /// Phase 2: merge aggregation — takes intermediate aggregate states from Phase 1, produces
    /// final results. Uses MergingAggregatedStep which natively expects intermediate state types
    /// (e.g. AggregateFunction(count)) in the input header, unlike AggregatingStep with
    /// requestOnlyMergeForAggregateProjection which adapts them to finalized types.
    auto merge_params = agg_step->getParams();
    merge_params.only_merge = true;
    auto merge_step_ptr = std::make_unique<MergingAggregatedStep>(
        partial_step->getOutputHeader(),
        std::move(merge_params),
        agg_step->getGroupingSetsParamsList(),
        /*final_=*/true,
        /*memory_efficient_aggregation_=*/false,
        /*memory_efficient_merge_threads_=*/0,
        agg_step->shouldProduceResultsInBucketOrder(),
        agg_step->getMaxBlockSize(),
        agg_step->getMaxBlockSizeForAggregationInOrder(),
        agg_step->usingMemoryBoundMerging());
    merge_step_ptr->setStepDescription(fmt::format("Merge: {}", agg_step->getStepDescription()), 200);

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
