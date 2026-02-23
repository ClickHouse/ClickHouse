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
    const UInt64 sort_limit = required_properties.sort_limit;
    SortingStep::Settings sort_settings(65000);
    sort_settings.temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;   /// TODO: construct from settings
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();

    /// Create a SortingStep expression whose input requires the same distribution
    /// as the source expression but with sorting relaxed to empty.
    ExpressionProperties input_required = expression->properties;
    input_required.sorting = {};
    input_required.sort_limit = 0;

    auto sort_expr = std::make_shared<GroupExpression>(
        std::make_unique<SortingStep>(input_header, sort_desc, sort_limit, sort_settings));
    sort_expr->group_id = expression->group_id;
    sort_expr->inputs.push_back({
        .group_id = expression->group_id,
        .required_properties = input_required});
    sort_expr->properties = expression->properties;
    sort_expr->properties.sorting = sort_desc;
    sort_expr->properties.sort_limit = sort_limit;

    sort_expr->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(sort_expr);
    return {sort_expr};
}

OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
