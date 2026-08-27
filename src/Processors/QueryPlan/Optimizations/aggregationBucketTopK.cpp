#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// The per-bucket selection scans every cell anyway; past this many candidate rows per bucket
/// the saved materialization no longer pays for the selection scan.
constexpr size_t max_bucket_top_k = 65536;

/// Traces an output column of a pass-through expression to the input column it forwards:
/// aliases are unwrapped, anything computed disqualifies.
const String * traceThroughExpression(const ExpressionStep & expression, const String & name)
{
    const auto * node = expression.getExpression().tryFindInOutputs(name);
    if (!node)
        return nullptr;
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return nullptr;
    return &node->result_name;
}

/// Whether the threshold merge may rank groups by values of this type. The `Subadditive` bound
/// does arithmetic on the values, so they must be `UInt64` (which is what both `count` and
/// `uniqExact` return). The extremum bounds only compare values, but the comparison of the
/// peeked values (`IColumn::compareAt`) must order them exactly like the merge of the states
/// does, which excludes floating point (the merge ignores NaNs, and which NaN order matches it
/// depends on the direction) and types whose single-value state falls back to `Field` ordering
/// with quirks (nothing nullable can appear: the plan only sees non-nullable results here).
/// `String` and `FixedString` are also excluded: the merge peeks every cell's partial value
/// into a column up front, and for them that is an extra full copy of the ordering payload
/// before anything is pruned - a memory regression the fixed-width scalars do not have.
bool isThresholdTopKValueType(MergedValueBound bound, const DataTypePtr & type)
{
    WhichDataType which(type);
    if (bound == MergedValueBound::Subadditive)
        return which.isUInt64();
    return which.isInt() || which.isUInt() || which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64()
        || which.isDecimal() || which.isEnum();
}

}

size_t tryPushBucketTopKIntoAggregation(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    /// The shape: Limit over Sorting (with a pushed-down limit, by one plain column) over zero
    /// or more pass-through expressions over a final aggregation, and the sort column is one of
    /// the aggregation's outputs. Serves two optimizations: the top-K threshold merge, for any
    /// aggregate with a declared merged-value bound, and the bucket-local Top-K conversion, for
    /// the lone-`count()` output. `HAVING`, `WITH TOTALS`, `LIMIT BY` and windows sit between
    /// the aggregation and the sorting as their own steps and break the adjacency.
    const auto * limit = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit || parent_node->children.size() != 1)
        return 0;

    /// The per-bucket selection permanently drops the other groups, so it cannot serve
    /// `LIMIT WITH TIES` (the output size is not known in advance) or an exact
    /// `rows_before_limit_at_least` (`alwaysReadTillEnd`): the counter runs downstream of the
    /// conversion and would report the kept rows instead of the full group count.
    if (limit->withTies() || limit->alwaysReadTillEnd())
        return 0;

    QueryPlan::Node * sorting_node = parent_node->children.front();
    const auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting || sorting_node->children.size() != 1)
        return 0;

    const size_t n = sorting->getLimit();
    if (n == 0 || n > max_bucket_top_k)
        return 0;

    const auto & description = sorting->getSortDescription();
    if (description.size() != 1 || description.front().with_fill)
        return 0;

    String column = description.front().column_name;
    const bool ascending = description.front().direction > 0;

    QueryPlan::Node * node = sorting_node->children.front();
    while (const auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        const String * traced = traceThroughExpression(*expression, column);
        if (!traced || node->children.size() != 1)
            return 0;
        column = *traced;
        node = node->children.front();
    }

    auto * aggregating = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating)
        return 0;

    const auto & params = aggregating->getParams();
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || params.overflow_row || params.keys_size == 0)
        return 0;

    /// The selection reads the count straight from the aggregate state and handles no
    /// null-key cell, so nullable and low-cardinality keys stay on the ordinary conversion.
    const auto & header = node->step->getOutputHeader();
    for (const auto & key : params.keys)
    {
        const auto & type = header->getByName(key).type;
        if (type->isNullable() || type->isLowCardinalityNullable() || typeid_cast<const DataTypeLowCardinality *>(type.get()))
            return 0;
    }

    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        if (aggregate.column_name != column)
            continue;

        /// The top-K threshold merge (see `Aggregator::Params::threshold_top_k`) serves any
        /// aggregate with a declared merged-value bound; at run time it yields to the lone
        /// count's conversion-stage selection below (a plain scan there beats the value-peeking
        /// walk) and stands down in a few other cases (single-level tables, dataflow statistics
        /// collection).
        bool threshold_top_k_enabled = false;
        if (settings.aggregation_top_k_threshold_merge && !description.front().collator)
        {
            const auto bound = aggregate.function->getMergedValueBound();
            /// The `Subadditive` bound serves only the descending order: for the ascending one
            /// its threshold (the smallest head) stays at the level of the typical partial value,
            /// which for near-uniform data never rises above the candidates, so the merge would
            /// degenerate into visiting every group. The extremum bounds are exact and converge
            /// right after the candidate heap fills in either direction.
            const bool bound_serves_direction = bound == MergedValueBound::Maximum || bound == MergedValueBound::Minimum
                || (bound == MergedValueBound::Subadditive && !ascending);
            if (bound_serves_direction && isThresholdTopKValueType(bound, aggregate.function->getResultType()))
            {
                aggregating->enableThresholdTopK(
                    Aggregator::Params::ThresholdTopKParams{
                        .k = n, .ascending = ascending, .aggregate_index = i, .bound = bound});
                threshold_top_k_enabled = true;
            }
        }

        if (!settings.aggregation_bucket_top_k)
            return 0;

        if (aggregate.function->getName() != "count" || !aggregate.argument_names.empty() || !aggregate.parameters.empty())
            return 0;

        /// The conversion-stage selection by the count (`bucket_top_k`) and the threshold merge
        /// are mutually exclusive at run time (the merge yields when `bucket_top_k` is set). For
        /// the lone count the selection wins: a plain scan of the merged bucket beats the
        /// value-peeking walk. But when other aggregates ride along, the threshold merge is the
        /// better deal - it also skips merging the losers' other states - so the selection steps
        /// aside for it (and still serves the shapes the merge does not, e.g. the ascending order).
        if (threshold_top_k_enabled && params.aggregates.size() > 1)
            return 0;

        aggregating->enableBucketTopK(n, ascending, i);
        return 0;
    }

    return 0;
}

}
