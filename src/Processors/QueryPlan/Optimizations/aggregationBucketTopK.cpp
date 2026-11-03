#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

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
    /// The shape is a top-K of the groups of a final aggregation by one of its aggregates (see `findAggregationTopK`).
    /// `HAVING`, `WITH TOTALS`, `LIMIT BY` and windows sit between the aggregation and the sorting as their own steps
    /// and break it, and an in-order aggregation, which the shape excludes, has no two-level conversion to run the
    /// selection in. Serves two optimizations: the top-K threshold merge, for any aggregate with a declared
    /// merged-value bound, and the bucket-local Top-K conversion, for the output of the aggregation's lone `count`,
    /// `uniqExact` or `uniqExactIf`.
    const auto top_k = findAggregationTopK(*parent_node);
    if (!top_k || top_k->limit > Aggregator::Params::max_bucket_top_k)
        return 0;

    auto * aggregating = top_k->aggregating;
    const auto & params = aggregating->getParams();
    const size_t n = top_k->limit;
    const bool ascending = top_k->ascending;

    /// The selection reads the count from the aggregate state and handles no null-key cell, so
    /// nullable and low-cardinality keys stay on the ordinary conversion.
    const auto & header = top_k->aggregating_node->step->getOutputHeader();
    for (const auto & key : params.keys)
    {
        const auto & type = header->getByName(key).type;
        if (type->isNullable() || type->isLowCardinalityNullable() || typeid_cast<const DataTypeLowCardinality *>(type.get()))
            return 0;
    }

    const size_t i = top_k->rank_index;
    const auto & aggregate = params.aggregates[i];

    /// The top-K threshold merge (see `Aggregator::Params::threshold_top_k`) serves any
    /// aggregate with a declared merged-value bound; at run time it yields to the lone
    /// count's conversion-stage selection below (a plain scan there beats the value-peeking
    /// walk) and stands down in a few other cases (single-level tables, dataflow statistics
    /// collection).
    /// The in-order aggregation never runs the threshold merge (see `AggregatingStep::applyOrder`,
    /// which also drops it when the order is applied after this pass).
    if (!top_k->has_collator && !aggregating->inOrder())
    {
        const auto bound = aggregate.function->getMergedValueBound();
        /// The `Subadditive` bound serves only the descending order. It is one-sided (see
        /// `MergedValueBound`): it bounds the merged value from above, which is what the
        /// descending threshold needs. The ascending order would instead need the lower
        /// half, which the wrapping `UInt64` accumulators of `count` and `sum` cannot
        /// promise - and it would not pay off anyway: its threshold (the smallest head)
        /// stays at the level of the typical partial value, which for near-uniform data
        /// never rises above the candidates, so the merge would degenerate into visiting
        /// every group. The extremum bounds are exact, but they converge only in the direction
        /// of the extremum itself: `max` descending and `min` ascending, where the merged value
        /// of a group equals its best partial value, so the walk stops right after the
        /// candidate heap fills. In the opposite direction (`max` ascending, `min` descending)
        /// the merged value of a group is its worst partial value, which can sit far behind
        /// the partial values the walk pops, so the threshold stays open across several tables
        /// and the walk degenerates into visiting most of the groups.
        const bool bound_serves_direction = (bound == MergedValueBound::Maximum && !ascending)
            || (bound == MergedValueBound::Minimum && ascending) || (bound == MergedValueBound::Subadditive && !ascending);
        if (bound_serves_direction && isThresholdTopKValueType(bound, aggregate.function->getResultType()))
        {
            aggregating->enableThresholdTopK(
                Aggregator::Params::ThresholdTopKParams{
                    .k = n, .ascending = ascending, .aggregate_index = i, .bound = bound});
        }
    }

    /// The conversion-stage selection ranks by a count it reads without converting the group (`uniqExact`
    /// finalizes from its set), one that never exceeds the rows of its group (see `isRowBoundedCount`). The
    /// adaptive aggregation also bounds these counts by the rows of a group to skip groups (see
    /// `AdaptiveTopKPruning`).
    if (!isRowBoundedCount(aggregate))
        return 0;

    /// The conversion-stage selection by the count (`bucket_top_k`) is enabled alongside the
    /// threshold merge; at run time they are mutually exclusive per bucket. For the lone count
    /// the selection wins outright: a plain scan of the merged bucket beats the value-peeking
    /// walk. When other aggregates ride along, the threshold merge is tried first - it also
    /// skips merging the losers' other states - and when it declines (e.g. the groups are
    /// split across the per-thread tables), the bucket is merged as usual and the selection
    /// still trims its conversion.
    ///
    /// `AggregatingStep::serialize` does not write the bucket top-K fields, so a serialized or
    /// distributed plan would show the selection without executing it. The threshold merge above
    /// stays: the side that runs the aggregation derives it again from its own plan.
    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return 0;

    aggregating->enableBucketTopK(n, ascending, i);
    return 0;
}

}
