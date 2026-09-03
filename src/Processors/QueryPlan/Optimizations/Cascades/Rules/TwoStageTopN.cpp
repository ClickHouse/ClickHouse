#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <fmt/format.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Splits a top-N `SortingStep(limit=L)` into a per-node bounded sort plus a coordinator
/// `Limit(L)` over the sorted-merged result, mirroring `TwoStageAggregationTransformation`:
///   SortingStep(limit=L) @ N nodes -> sorted GatherExchange -> Limit(L) @ 1 node
/// The coordinator `Limit` makes this group honor its "top-L" contract independently of any
/// outer Limit; the outer Limit still applies the exact n / offset / WITH TIES.
class TwoStageTopN : public IOptimizationRule
{
public:
    String getName() const override { return "TwoStageTopN"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const override
    {
        /// With `exact_rows_before_limit` the per-node sorts must feed the full row count
        /// into `rows_before_limit_at_least`, but the internal cap below cuts the pipeline
        /// walk that collects those counters, so the query would report fewer rows.
        if (memo.getContext().exact_rows_before_limit)
            return false;
        /// Skip the partial we create ourselves.
        return isTopNSort(*expression->getQueryPlanStep())
            && !assert_cast<const SortingStep &>(*expression->getQueryPlanStep()).isPartialTopN();
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

    /// Phase 1: per-node bounded sort. Same step, marked so it is implemented per node
    /// (TopNImplementation) and not split again.
    auto partial_step = cloneStepAs(*sorting_step);
    partial_step->setPartialTopN();
    GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(std::move(partial_step));

    /// Phase 2: coordinator limit over the sorted-merged partial runs. Its input requires the
    /// same sorting at a single node, so DistributionEnforcer inserts a sorted-merge gather.
    auto limit_step = std::make_unique<LimitStep>(sorting_step->getOutputHeader(), limit, /*offset_=*/0);
    limit_step->setStepDescription(fmt::format("TopN merge {}", sorting_step->getStepDescription()), 200);

    ExpressionProperties merge_input_required;
    merge_input_required.sorting = sort_desc;
    merge_input_required.distribution.node_count = 1;
    auto final_expr = addTwoStageSplit(memo, expression, std::move(partial_expr), std::move(limit_step), std::move(merge_input_required));

    if (!final_expr)
        return {};
    return {final_expr};
}

OptimizationRulePtr createTwoStageTopN() { return std::make_shared<TwoStageTopN>(); }

}
