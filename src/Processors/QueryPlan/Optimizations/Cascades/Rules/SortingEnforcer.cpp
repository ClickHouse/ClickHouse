#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Core/SortDescription.h>
#include <Common/Exception.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    /// The environment carries the query's sort settings (size limits, spill thresholds), seeded
    /// at optimizer setup, so the enforcer-built sort matches the rest of the query's pipeline.
    const auto & captured_settings = memo.getEnvironment().sort_settings;
    if (!captured_settings)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SortingEnforcer has no sort settings; they must be seeded at optimizer setup");
    const SortingStep::Settings & sort_settings = *captured_settings;
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();

    /// Create a full SortingStep expression whose input requires the same distribution
    /// as the source expression but with sorting relaxed to empty.  Any row limit is owned
    /// by a separate Limit/top-N operator, not by the sorting property.
    ExpressionProperties input_required = expression->properties;
    input_required.sorting = {};

    ExpressionProperties output_properties = expression->properties;
    output_properties.sorting = sort_desc;
    auto sort_expr = makeEnforcerExpression(
        expression,
        std::make_unique<SortingStep>(input_header, sort_desc, /*limit=*/0, sort_settings),
        input_required,
        std::move(output_properties),
        EnforcerAxis::Sorting);

    std::vector<GroupExpressionPtr> result;
    addPhysicalToMemo(sort_expr, required_properties, memo, result);
    return result;
}

OptimizationRulePtr createSortingEnforcer();
OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
