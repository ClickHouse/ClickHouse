#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
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
    /// Without a required sort description there is nothing this enforcer can build.
    if (required_properties.sorting.empty())
        return false;
    /// The stream layout is also this enforcer's axis: a layout gap can exist alone (e.g.
    /// streams disjoint on more columns than required), and the sort it builds closes both
    /// gaps at once.
    return !ExpressionProperties::isSortingSatisfiedBy(required_properties.sorting, expression->properties.sorting)
        || !ExpressionProperties::isStreamLayoutSatisfiedBy(required_properties, expression->properties);
}

std::vector<GroupExpressionPtr> SortingEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const SortDescription & sort_desc = required_properties.sorting;
    /// The environment carries the query's sort settings (size limits, spill thresholds), seeded
    /// at optimizer setup, so the enforcer-built sort matches the rest of the query's pipeline.
    const auto & captured_settings = memo.getContext().sort_settings;
    if (!captured_settings)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SortingEnforcer has no sort settings; they must be seeded at optimizer setup");
    const SortingStep::Settings & sort_settings = *captured_settings;
    const auto & input_header = expression->getQueryPlanStep()->getOutputHeader();

    /// A disjointness requirement is met by splitting the streams by the required columns
    /// before sorting. The columns must form a prefix of the sort, so that every partition
    /// lands whole in one stream and each stream stays sorted. When they do not, the plain
    /// sort below still satisfies the requirement: one stream keeps every group whole.
    SortDescription partition_by;
    if (required_properties.stream_layout == StreamLayout::Disjoint)
    {
        for (const auto & sort_column : sort_desc)
        {
            if (partition_by.size() == required_properties.stream_disjoint_columns.size())
                break;
            bool is_disjoint_column = false;
            for (const auto & column_set : required_properties.stream_disjoint_columns)
            {
                if (column_set.contains(sort_column.column_name))
                {
                    is_disjoint_column = true;
                    break;
                }
            }
            if (!is_disjoint_column)
                break;
            partition_by.push_back(sort_column);
        }
        if (partition_by.size() != required_properties.stream_disjoint_columns.size())
            partition_by.clear();
    }

    /// Create a full SortingStep expression whose input requires the same distribution
    /// as the source expression but with sorting and layout relaxed.  Any row limit is owned
    /// by a separate Limit/top-N operator, not by the sorting property.
    ExpressionProperties input_required = expression->properties;
    input_required.sorting = {};
    input_required.stream_layout = StreamLayout::Unknown;
    input_required.stream_disjoint_columns.clear();

    /// The sort makes the stream layout itself: one merged stream, or streams split by the
    /// partition columns.
    ExpressionProperties output_properties = expression->properties;
    output_properties.sorting = sort_desc;
    if (partition_by.empty())
    {
        output_properties.stream_layout = StreamLayout::Single;
        output_properties.stream_disjoint_columns.clear();
    }
    else
        output_properties.setDisjointStreams(required_properties.stream_disjoint_columns);

    auto sorting_step = partition_by.empty()
        ? std::make_unique<SortingStep>(input_header, sort_desc, /*limit=*/0, sort_settings)
        : std::make_unique<SortingStep>(input_header, sort_desc, partition_by, /*limit=*/0, sort_settings);
    auto sort_expr = makeEnforcerExpression(
        expression,
        std::move(sorting_step),
        input_required,
        std::move(output_properties),
        EnforcedProperty::Sorting);

    return addPhysicalToMemo(sort_expr, required_properties, memo);
}

OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
