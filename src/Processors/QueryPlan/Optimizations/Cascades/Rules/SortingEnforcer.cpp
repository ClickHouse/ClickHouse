#include <Core/Defines.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Core/SortDescription.h>
#include <memory>

namespace DB
{

class SortingEnforcer : public IOptimizationRule
{
public:
    String getName() const override { return "SortingEnforcer"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool SortingEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isSortingSatisfiedBy(required_properties.sorting, expression->properties.sorting);
}

std::vector<GroupExpressionPtr> SortingEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const SortDescription & sort_desc = required_properties.sorting;
    const UInt64 sort_limit = required_properties.sort_limit;
    SortingStep::Settings sort_settings(65000);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;   /// TODO: construct from settings
    const size_t max_block_size = 65000;                /// TODO: construct from settings
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();
    const size_t node_count = expression->properties.distribution.node_count;

    std::vector<GroupExpressionPtr> result;

    if (node_count <= 1)
    {
        /// Single node: sort locally.
        auto local_expr = std::make_shared<GroupExpression>(*expression);
        local_expr->property_enforcer_steps.push_back(
            std::make_unique<SortingStep>(input_header, sort_desc, sort_limit, sort_settings));
        local_expr->properties.sorting = sort_desc;
        local_expr->inputs = expression->inputs;
        local_expr->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(local_expr);
        result.push_back(local_expr);
        return result;
    }

    /// Multi-node: Strategy A — gather all data to one node, then sort locally.
    /// Simple but moves all data before sorting.
    {
        auto strategy_a = std::make_shared<GroupExpression>(*expression);
        strategy_a->property_enforcer_steps.push_back(
            std::make_unique<GatherExchangeStep>(input_header, node_count));
        strategy_a->property_enforcer_steps.push_back(
            std::make_unique<SortingStep>(input_header, sort_desc, sort_limit, sort_settings));
        strategy_a->properties.sorting = sort_desc;
        strategy_a->properties.distribution.node_count = 1;
        strategy_a->inputs = expression->inputs;
        strategy_a->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(strategy_a);
        result.push_back(strategy_a);
    }

    /// Multi-node: Strategy B — sort on each node, gather with sort order preserved, final merge.
    /// Each node independently sorts its partition. A sorted-merge gather collects the streams.
    /// A final MergingSorted step applies the limit and ensures a single fully-sorted output.
    /// For sort_limit > 0, each node keeps only sort_limit rows, reducing network traffic.
    {
        auto strategy_b = std::make_shared<GroupExpression>(*expression);
        /// Partial sort on each node.
        strategy_b->property_enforcer_steps.push_back(
            std::make_unique<SortingStep>(input_header, sort_desc, sort_limit, sort_settings));
        /// Gather preserving sort order across nodes.
        strategy_b->property_enforcer_steps.push_back(
            std::make_unique<GatherExchangeStep>(input_header, node_count, sort_desc));
        /// Final merge-sort to produce a single sorted stream and apply the limit.
        strategy_b->property_enforcer_steps.push_back(
            std::make_unique<SortingStep>(input_header, sort_desc, max_block_size, sort_limit));
        strategy_b->properties.sorting = sort_desc;
        strategy_b->properties.distribution.node_count = 1;
        strategy_b->inputs = expression->inputs;
        strategy_b->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(strategy_b);
        result.push_back(strategy_b);
    }

    return result;
}

OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
