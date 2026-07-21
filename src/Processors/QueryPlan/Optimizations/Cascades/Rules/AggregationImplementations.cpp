#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Core/SortDescription.h>
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
///     (only applicable when node_count > 1 and there are `GROUP BY` keys)
///   - Partial: a non-final aggregation stays where its input is (any node count)
///
/// The two-stage split (partial + merge) is handled separately by
/// `TwoStageAggregationTransformation`, which splits a logical aggregation into a
/// final-merge over a partial before implementations are assigned.
class AggregationImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "Aggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return false; }

    class StrategyEnumerator;

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

/// Emits the physical alternatives of one logical aggregation into the memo, one method
/// per strategy.
class AggregationImplementation::StrategyEnumerator
{
public:
    StrategyEnumerator(
        const AggregationImplementation & rule_,
        GroupExpressionPtr expression_,
        const ExpressionProperties & required_properties_,
        Memo & memo_,
        std::vector<GroupExpressionPtr> & result_);

    void addPartialAggregation(size_t node_count);
    void addLocalAggregation();
    void addShuffleAggregation(size_t node_count);
    void addSingleKeyShuffleAggregations(const std::vector<size_t> & candidate_node_counts);

    /// See `addShuffleAggregation` for why each condition disables the shuffle strategy.
    bool isShuffleApplicable() const
    {
        return !agg_step.getParams().keys.empty()
            && !agg_step.isGroupingSets()
            && !agg_step.getParams().overflow_row
            && agg_step.getParams().max_rows_to_group_by == 0;
    }

    bool hasMultipleKeys() const { return agg_step.getParams().keys.size() >= 2; }

private:
    /// Clones the aggregation step with the given description into a physical alternative
    /// with the given strategy and distributions and offers it to the memo.
    void addAlternative(
        ImplementationStrategyPtr strategy,
        const DistributionDescription & input_dist,
        const DistributionDescription & output_dist,
        String description = {});

    const AggregationImplementation & rule;
    GroupExpressionPtr expression;
    const AggregatingStep & agg_step;
    const ExpressionProperties & required_properties;
    Memo & memo;
    std::vector<GroupExpressionPtr> & result;
};

/// Logical transformation that splits a single-phase Agg into a two-phase plan:
///   PartialAgg(input) -> ShuffleExchange -> FinalMergeAgg
///
/// PartialAgg computes partial aggregate states locally on each node without finalization.
/// FinalMergeAgg receives pre-aggregated states and merges them into final results.
/// The exchange between the two is inserted by the `DistributionEnforcer` based on the
/// distribution requirements set by the implementation rules on FinalMergeAgg.
///
/// This split is only attempted for aggregations that support it (`canUseProjection`).
class TwoStageAggregationTransformation : public IOptimizationRule
{
public:
    String getName() const override { return "TwoStageAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};


bool AggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step != nullptr &&
        expression->strategy == nullptr;
}

AggregationImplementation::StrategyEnumerator::StrategyEnumerator(
    const AggregationImplementation & rule_,
    GroupExpressionPtr expression_,
    const ExpressionProperties & required_properties_,
    Memo & memo_,
    std::vector<GroupExpressionPtr> & result_)
    : rule(rule_)
    , expression(std::move(expression_))
    , agg_step(*typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep()))
    , required_properties(required_properties_)
    , memo(memo_)
    , result(result_)
{
}

void AggregationImplementation::StrategyEnumerator::addAlternative(
    ImplementationStrategyPtr strategy,
    const DistributionDescription & input_dist,
    const DistributionDescription & output_dist,
    String description)
{
    auto new_step = agg_step.clone();
    if (description.empty())
        new_step->setStepDescription(agg_step);
    else
        new_step->setStepDescription(std::move(description), 200);

    GroupExpressionPtr alternative = std::make_shared<GroupExpression>(*expression);
    alternative->plan_step = std::move(new_step);
    alternative->strategy = std::move(strategy);
    alternative->inputs[0].required_properties.distribution = input_dist;
    alternative->properties.distribution = output_dist;

    rule.addPhysicalToMemo(alternative, required_properties, memo, result);
}

/// Partial (non-final) aggregation at the given node count. The output distribution is
/// `{node_count, []}` (no column guarantee).
void AggregationImplementation::StrategyEnumerator::addPartialAggregation(size_t node_count)
{
    DistributionDescription dist;
    dist.node_count = node_count;
    addAlternative(std::make_shared<PartialAggregationStrategy>(), dist, dist);
}

/// Local - gather all input to one node, aggregate there.
/// Always applicable; when the cluster has only 1 node it is also the only meaningful strategy.
void AggregationImplementation::StrategyEnumerator::addLocalAggregation()
{
    DistributionDescription single_node;    /// node_count=1 (default)
    addAlternative(std::make_shared<LocalAggregationStrategy>(), single_node, single_node,
        fmt::format("Local {}", agg_step.getStepDescription()));
}

/// Shuffle - input pre-distributed by group keys, each node aggregates its
/// own partition of keys and produces a final result independently.
/// Not applicable for global aggregations (e.g. `COUNT(*)`) that have no group keys.
/// Not applicable for `GROUPING SETS`: `params.keys` is the union of all sets' keys,
/// so shuffling by the union splits the rows of one grouping-set group across nodes.
/// Not applicable with an overflow row: each node would emit its own overflow row.
/// Not applicable with `max_rows_to_group_by`: the limit is a global contract, but each
/// node would enforce it independently, so the result could exceed it (or skip the
/// expected exception). Keep such aggregations local.
void AggregationImplementation::StrategyEnumerator::addShuffleAggregation(size_t node_count)
{
    DistributionDescription by_keys;
    by_keys.node_count = node_count;
    for (const auto & key : agg_step.getParams().keys)
        by_keys.columns.push_back({key});

    addAlternative(std::make_shared<ShuffleAggregationStrategy>(), by_keys, by_keys,
        fmt::format("Shuffle {}", agg_step.getStepDescription()));
}

/// Single-key shuffle alternatives.
/// For aggregations with 2+ group-by keys, generate a shuffle alternative for each
/// individual key. Correctness: `GROUP BY (A, B)` with data shuffled by `A` is correct
/// because all rows with the same `(A, B)` have the same `A`, hence the same node.
void AggregationImplementation::StrategyEnumerator::addSingleKeyShuffleAggregations(const std::vector<size_t> & candidate_node_counts)
{
    for (const auto & single_key : agg_step.getParams().keys)
    {
        for (size_t candidate_node_count : candidate_node_counts)
        {
            DistributionDescription by_single_key;
            by_single_key.node_count = candidate_node_count;
            by_single_key.columns.push_back({single_key});

            addAlternative(std::make_shared<ShuffleAggregationStrategy>(), by_single_key, by_single_key,
                fmt::format("Shuffle (by {}) {}", single_key, agg_step.getStepDescription()));
        }
    }
}

std::vector<GroupExpressionPtr> AggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationImplementation::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AggregationImplementation::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    const size_t cluster_node_count = memo.getEnvironment().cluster_node_count;
    const auto candidate_node_counts = getCandidateNodeCounts(cluster_node_count);

    std::vector<GroupExpressionPtr> result;
    StrategyEnumerator strategies(*this, expression, required_properties, memo, result);

    /// Partial (non-final) aggregation: create distributed implementations at each candidate
    /// node count. When the parent (MergingAggregated) requires `{1 node}`, the
    /// `DistributionEnforcer` bridges the gap via `GatherExchange` - on the partial
    /// output (~25 rows) rather than the raw input (~1M rows). This produces:
    ///   ParallelRead -> Expression -> PartialAgg({N nodes}) -> GatherExchange -> MergeAgg
    /// We intentionally do NOT create a `{1 node}` variant for multi-node clusters: if one
    /// existed, it would become the best for `{1 node}` immediately, preventing the enforcer
    /// from ever running and producing the cheaper GatherExchange-on-partial-output plan.
    /// However, for a single-node cluster we MUST create the `{1 node}` variant because there
    /// are no multi-node candidates and no enforcer path to bridge the gap.
    if (!agg_step->getFinal())
    {
        auto partial_candidates = candidate_node_counts;
        if (partial_candidates.empty())
            partial_candidates.push_back(1);

        for (size_t candidate_node_count : partial_candidates)
            strategies.addPartialAggregation(candidate_node_count);
        return result;
    }

    /// `distributed_plan_force_shuffle_aggregation` leaves shuffle as the only strategy
    /// on a multi-node cluster whenever it is applicable.
    const bool only_shuffle = memo.getEnvironment().distributed_plan_force_shuffle_aggregation
        && strategies.isShuffleApplicable() && !candidate_node_counts.empty();

    if (!only_shuffle)
        strategies.addLocalAggregation();

    /// For a single-node cluster distributed strategies are identical to local - skip them.
    if (candidate_node_counts.empty())
        return result;

    if (strategies.isShuffleApplicable())
    {
        for (size_t candidate_node_count : candidate_node_counts)
            strategies.addShuffleAggregation(candidate_node_count);

        if (strategies.hasMultipleKeys())
            strategies.addSingleKeyShuffleAggregations(candidate_node_counts);
    }

    return result;
}

OptimizationRulePtr createAggregationImplementation();
OptimizationRulePtr createAggregationImplementation() { return std::make_shared<AggregationImplementation>(); }


bool TwoStageAggregationTransformation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    return agg_step != nullptr &&
        expression->strategy == nullptr &&
        agg_step->getFinal() &&
        !agg_step->isGroupingSets() &&           /// distributed merging of grouping-set states is not supported
        !agg_step->getParams().overflow_row &&
        agg_step->getParams().max_rows_to_group_by == 0 &&  /// global row limit must be enforced by one aggregator
        !agg_step->getParams().only_merge &&     /// don't split a merge step that's already from a prior split
        /// `distributed_plan_force_shuffle_aggregation` forbids the partial + merge split
        /// whenever the shuffle strategy is available (the aggregation has group keys).
        !(memo.getEnvironment().distributed_plan_force_shuffle_aggregation && !agg_step->getParams().keys.empty());
}

std::vector<GroupExpressionPtr> TwoStageAggregationTransformation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoStageAggregationTransformation::applyImpl called for non-AggregatingStep expression '{}'",
            expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoStageAggregationTransformation::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    /// Phase 1: partial aggregation - takes raw rows, outputs intermediate aggregate states.
    auto partial_step_ptr = cloneStepAs(*agg_step);
    auto * partial_step = partial_step_ptr.get();
    partial_step->setFinal(false);
    /// The memory-efficient merge below expects every input to deliver two-level buckets in
    /// ascending order. Force the partial step to emit them that way; otherwise a parallel
    /// flush unites several bucket sequences into one exchange stream out of order and the
    /// merge emits some groups twice.
    if (memo.getEnvironment().distributed_aggregation_memory_efficient)
        partial_step->setShouldProduceResultsInBucketOrder(true);
    partial_step->setStepDescription(fmt::format("Partial: {}", agg_step->getStepDescription()), 200);

    /// Phase 2: merge aggregation - takes intermediate aggregate states from Phase 1, produces
    /// final results. Uses MergingAggregatedStep which natively expects intermediate state types
    /// (e.g. AggregateFunction(count)) in the input header, unlike AggregatingStep with
    /// requestOnlyMergeForAggregateProjection which adapts them to finalized types.
    auto merge_params = agg_step->getParams();
    merge_params.only_merge = true;
    /// Grouping sets never reach this rule (see checkPattern), so the memory-efficient
    /// mode needs no grouping-sets guard here, unlike the rule-based planner.
    auto merge_step_ptr = std::make_unique<MergingAggregatedStep>(
        partial_step->getOutputHeader(),
        std::move(merge_params),
        agg_step->getGroupingSetsParamsList(),
        /*final_=*/true,
        memo.getEnvironment().distributed_aggregation_memory_efficient,
        agg_step->getTemporaryDataMergeThreads(),
        agg_step->shouldProduceResultsInBucketOrder(),
        agg_step->getMaxBlockSize(),
        agg_step->getMaxBlockSizeForAggregationInOrder(),
        agg_step->usingMemoryBoundMerging());
    merge_step_ptr->setStepDescription(fmt::format("Merge: {}", agg_step->getStepDescription()), 200);

    /// The partial aggregation becomes its own group over the original inputs; the merge becomes
    /// a logical alternative in the original group. The merge's implementation rules will set the
    /// distribution requirements (Local or Shuffle), causing the `DistributionEnforcer` to insert
    /// the appropriate exchange before the partial step.
    GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(std::move(partial_step_ptr));
    auto merge_expr = addTwoStageSplit(memo, expression, std::move(partial_expr), std::move(merge_step_ptr), {});

    return {merge_expr};
}

OptimizationRulePtr createTwoStageAggregationTransformation();
OptimizationRulePtr createTwoStageAggregationTransformation() { return std::make_shared<TwoStageAggregationTransformation>(); }

}
