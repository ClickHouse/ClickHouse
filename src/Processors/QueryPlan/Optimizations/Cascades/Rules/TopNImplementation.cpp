#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>
#include <fmt/format.h>
#include <memory>

namespace DB
{

/// A top-N is a `SortingStep::Full` carrying a limit (a bounded sort).  The limit is owned by
/// this operator, not by the sorting property.

/// Implements a top-N sort either on a single node (gather the input, then bounded sort) or
/// per node (bounded sort on each node, output stays distributed and sorted).  The per-node
/// variant lets `DistributionEnforcer` add a sorted-merge gather, so only each node's top-N
/// rows cross the network.  Per-node sorting is only valid for the partial of a two-stage
/// top-N (`TwoStageTopN`), where a coordinator limit re-applies the global bound afterwards;
/// the original operator must keep the whole result, so it is implemented single-node only.
class TopNImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "TopN"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        return isTopNSort(*expression->getQueryPlanStep());
    }
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

std::vector<GroupExpressionPtr> TopNImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
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

    const bool is_partial = sorting_step->isPartialTopN();
    if (is_partial)
    {
        /// Bounded sort on each node; a sorted gather merges and a coordinator limit re-bounds.
        for (size_t candidate : getCandidateNodeCounts(memo.getContext().cluster_node_count))
            make_variant(candidate);
    }
    else
    {
        /// Single-node bounded sort: the input is gathered to one node and sorted there.
        make_variant(1);
    }

    return result;
}

OptimizationRulePtr createTopNImplementation() { return std::make_shared<TopNImplementation>(); }

}
