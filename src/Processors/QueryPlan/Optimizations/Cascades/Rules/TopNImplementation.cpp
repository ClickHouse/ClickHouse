#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <fmt/format.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A top-N is a `SortingStep::Full` carrying a limit (a bounded sort).  The limit is owned by
/// this operator, not by the sorting property.

/// Implements a top-N sort either on a single node (gather the input, then bounded sort) or
/// per shard (bounded sort on each node, output stays distributed and sorted).  The per-shard
/// variant lets `DistributionEnforcer` add a sorted-merge gather, so only each shard's top-N
/// rows cross the network.  Per-shard sorting is only valid for the partial of a two-stage
/// top-N (`TwoStageTopN`), where a coordinator limit re-applies the global bound afterwards;
/// the original operator must keep the whole result, so it is implemented single-node only.
class SortImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "SortImplementation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return isTopNSort(*expression->getQueryPlanStep());
    }
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

std::vector<GroupExpressionPtr> SortImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(expression->getQueryPlanStep());
    const SortDescription sort_desc = sorting_step->getSortDescription();
    std::vector<GroupExpressionPtr> result;

    auto make_variant = [&](size_t node_count)
    {
        auto impl = std::make_shared<GroupExpression>(*expression);

        chassert(impl->inputs.size() == 1);
        ExpressionProperties input_required;
        input_required.distribution.node_count = node_count;   /// raw, unsorted input
        impl->inputs[0].required_properties = input_required;

        impl->properties = ExpressionProperties{};
        impl->properties.distribution.node_count = node_count;
        impl->properties.sorting = sort_desc;                  /// output is sorted

        addPhysicalToMemo(impl, required_properties, memo, result);
    };

    const bool is_partial = dynamic_cast<const PartialTopNStrategy *>(expression->strategy.get()) != nullptr;
    if (is_partial)
    {
        /// Bounded sort on each shard; a sorted gather merges and a coordinator limit re-bounds.
        for (size_t candidate : getCandidateNodeCounts(memo.getEnvironment().cluster_node_count))
            make_variant(candidate);
    }
    else
    {
        /// Single-node bounded sort: the input is gathered to one node and sorted there.
        make_variant(1);
    }

    return result;
}

/// Splits a top-N `SortingStep(limit=L)` into a per-shard bounded sort plus a coordinator
/// `Limit(L)` over the sorted-merged result, mirroring `TwoPhaseAggregationTransformation`:
///   SortingStep(limit=L) @ N nodes -> sorted GatherExchange -> Limit(L) @ 1 node
/// The coordinator `Limit` makes this group honor its "top-L" contract independently of any
/// outer Limit; the outer Limit still applies the exact n / offset / WITH TIES.
class TwoStageTopN : public IOptimizationRule
{
public:
    String getName() const override { return "TwoStageTopN"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const override
    {
        /// With `exact_rows_before_limit` the per-shard sorts must feed the full row count
        /// into `rows_before_limit_at_least`, but the internal cap below cuts the pipeline
        /// walk that collects those counters, so the query would report fewer rows.
        if (memo.getEnvironment().exact_rows_before_limit)
            return false;
        /// Skip the partial we create ourselves (it carries PartialTopNStrategy).
        return isTopNSort(*expression->getQueryPlanStep()) && expression->strategy == nullptr;
    }
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

std::vector<GroupExpressionPtr> TwoStageTopN::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(expression->getQueryPlanStep());
    if (!sorting_step)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoStageTopN::applyImpl called for non-SortingStep expression '{}'", expression->getDescription());
    if (expression->inputs.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TwoStageTopN::applyImpl: expected 1 input, got {} for expression '{}'",
            expression->inputs.size(), expression->getDescription());

    const UInt64 limit = sorting_step->getLimit();
    const SortDescription sort_desc = sorting_step->getSortDescription();

    /// Phase 1: per-shard bounded sort. Same step, marked so it is implemented per shard
    /// (SortImplementation) and not split again.
    GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(sorting_step->clone());
    partial_expr->strategy = std::make_shared<PartialTopNStrategy>();

    /// Phase 2: coordinator limit over the sorted-merged partial runs. Its input requires the
    /// same sorting at a single node, so DistributionEnforcer inserts a sorted-merge gather.
    auto limit_step = std::make_unique<LimitStep>(sorting_step->getOutputHeader(), limit, /*offset_=*/0);
    limit_step->setStepDescription(fmt::format("TopN merge {}", sorting_step->getStepDescription()), 200);

    ExpressionProperties merge_input_required;
    merge_input_required.sorting = sort_desc;
    merge_input_required.distribution.node_count = 1;
    auto final_expr = addTwoStageSplit(memo, expression, std::move(partial_expr), std::move(limit_step), std::move(merge_input_required));

    return {final_expr};
}

OptimizationRulePtr createSortImplementation();
OptimizationRulePtr createSortImplementation() { return std::make_shared<SortImplementation>(); }
OptimizationRulePtr createTwoStageTopN();
OptimizationRulePtr createTwoStageTopN() { return std::make_shared<TwoStageTopN>(); }

}
