#include <Core/Defines.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Core/SortDescription.h>
#include <memory>

namespace DB
{

/// Produces a self-referential SortingStep enforcer expression.
/// The expression lives in the same group as the source and its single input
/// points back to the same group with relaxed properties (sorting removed).
///
/// Multi-node sorting strategies compose naturally with DistributionEnforcer:
///   - Strategy A (Gather -> Sort):  DistributionEnforcer produces Gather,
///     then SortingEnforcer adds Sort on top of the gathered (single-node) result.
///   - Strategy B (Sort-per-node -> SortedGather):  SortingEnforcer adds Sort on
///     each node, then DistributionEnforcer's sorted-merge gather variant
///     gathers while preserving sort order.
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
    SortingStep::Settings sort_settings(65000);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;   /// TODO: construct from settings
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();

    /// Create a full SortingStep expression whose input requires the same distribution
    /// as the source expression but with sorting relaxed to empty.  Any row limit is owned
    /// by a separate Limit/top-N operator, not by the sorting property.
    ExpressionProperties input_required = expression->properties;
    input_required.sorting = {};

    auto sort_expr = std::make_shared<GroupExpression>(
        std::make_unique<SortingStep>(input_header, sort_desc, /*limit=*/0, sort_settings));
    sort_expr->group_id = expression->group_id;
    sort_expr->inputs.push_back({
        .group_id = expression->group_id,
        .required_properties = input_required});
    sort_expr->properties = expression->properties;
    sort_expr->properties.sorting = sort_desc;

    sort_expr->setApplied(*this, required_properties);
    /// Skip scheduling a structural duplicate so it does not consume optimizer task budget.
    if (!memo.getGroup(expression->group_id)->addPhysicalExpression(sort_expr))
        return {};
    return {sort_expr};
}

OptimizationRulePtr createSortingEnforcer();
OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
