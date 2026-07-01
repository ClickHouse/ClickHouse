#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TopNAggregatingStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <climits>
#include <optional>

namespace DB::QueryPlanOptimizations
{

namespace
{

String traceColumnThroughDAG(const ActionsDAG & dag, const String & column_name)
{
    const auto * node = dag.tryFindInOutputs(column_name);
    if (!node)
        return {};

    while (true)
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
            return node->result_name;

        if (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        {
            node = node->children[0];
            continue;
        }

        return {};
    }
}

ReadFromMergeTree * findReadFromMergeTree(QueryPlan::Node & node)
{
    auto * current = &node;
    while (current)
    {
        if (auto * read = typeid_cast<ReadFromMergeTree *>(current->step.get()))
            return read;

        if (current->children.size() != 1)
            return nullptr;

        if (typeid_cast<ExpressionStep *>(current->step.get()))
        {
            current = current->children[0];
            continue;
        }

        return nullptr;
    }
    return nullptr;
}

/// Map the aggregate argument back to the original MergeTree column name through any
/// ExpressionSteps below the aggregation. Only pure pass-through / alias chains are followed:
/// if a step computes the argument (any non-INPUT, non-ALIAS node), tracing fails and we return
/// {} so the caller rejects the rewrite. Otherwise the read-order would be requested on the
/// storage column while the aggregate is over a derived value (e.g. `-ts AS ts`), which can pick
/// the wrong group.
String resolveOriginalArgName(const String & arg_name, QueryPlan::Node * child_node)
{
    String resolved = arg_name;
    while (child_node)
    {
        auto * child_expr = typeid_cast<ExpressionStep *>(child_node->step.get());
        if (!child_expr)
            break;

        resolved = traceColumnThroughDAG(child_expr->getExpression(), resolved);
        if (resolved.empty())
            break;

        if (child_node->children.size() != 1)
            break;
        child_node = child_node->children[0];
    }
    return resolved;
}

/// The matched query-plan shape: LimitStep -> SortingStep(single column) -> [ExpressionStep] -> AggregatingStep.
struct PlanMatch
{
    size_t limit = 0;
    SortColumnDescription sort_column;     /// the single ORDER BY column
    ExpressionStep * expr_step = nullptr;  /// optional column-rename expression between Sort and Aggregating
    QueryPlan::Node * expr_node = nullptr;
    QueryPlan::Node * agg_node = nullptr;
    AggregatingStep * aggregating_step = nullptr;
};

/// Match the LimitStep -> SortingStep -> [ExpressionStep] -> AggregatingStep chain, rejecting shapes
/// whose result, ordering, or exceptions early termination cannot reproduce: WITH TIES, OFFSET,
/// always_read_till_end (WITH TOTALS / exact rows-before-limit), sort size limits (max_rows_to_sort /
/// max_bytes_to_sort), grouping sets, overflow row, and max_rows_to_group_by.
bool matchPlan(QueryPlan::Node & node, PlanMatch & match)
{
    auto * limit_step = typeid_cast<LimitStep *>(node.step.get());
    if (!limit_step || limit_step->getLimit() == 0 || limit_step->withTies() || limit_step->getOffset() > 0
        || limit_step->alwaysReadTillEnd())
        return false;

    if (node.children.size() != 1)
        return false;

    auto * sort_node = node.children[0];
    auto * sorting_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sorting_step)
        return false;

    const auto & sort_desc = sorting_step->getSortDescription();
    if (sort_desc.size() != 1 || sort_node->children.size() != 1)
        return false;

    /// SortingStep enforces sort size limits (max_rows_to_sort / max_bytes_to_sort, with
    /// sort_overflow_mode). The fused path sorts the ~K early-terminated rows with a plain
    /// partial sort that would skip those checks, so reject when any such limit is set.
    if (sorting_step->getSettings().size_limits.hasLimits())
        return false;

    auto * after_sort = sort_node->children[0];
    ExpressionStep * expr_step = nullptr;
    QueryPlan::Node * expr_node = nullptr;
    QueryPlan::Node * agg_node = nullptr;

    if (auto * maybe_expr = typeid_cast<ExpressionStep *>(after_sort->step.get()))
    {
        expr_step = maybe_expr;
        expr_node = after_sort;
        if (expr_node->children.size() != 1)
            return false;
        agg_node = expr_node->children[0];
    }
    else
    {
        agg_node = after_sort;
    }

    auto * aggregating_step = typeid_cast<AggregatingStep *>(agg_node->step.get());
    if (!aggregating_step || aggregating_step->isGroupingSets())
        return false;

    const auto & params = aggregating_step->getParams();
    if (params.overflow_row || params.max_rows_to_group_by != 0)
        return false;
    /// An aggregate projection can flip the step into merge-state mode (input is AggregateFunction
    /// state columns, aggregates_positions unpopulated). The TopN transform only ever calls
    /// Aggregator::executeOnBlock, never mergeOnBlock, so it cannot consume state columns — bail out.
    if (params.only_merge)
        return false;
    if (params.keys.empty() || params.aggregates.empty())
        return false;

    match.limit = limit_step->getLimit();
    match.sort_column = sort_desc[0];
    match.expr_step = expr_step;
    match.expr_node = expr_node;
    match.agg_node = agg_node;
    match.aggregating_step = aggregating_step;
    return true;
}

/// The ORDER BY aggregate and the direction/argument constraints derived from it.
struct OrderByAggregate
{
    int required_direction = 0;
    bool output_ordered_by_sort_key = false;
    String determining_arg;  /// column whose sort order lets the first row decide the result
};

/// Resolve the (expression-traced) ORDER BY column to an aggregate, check its
/// first-row-determined direction matches the query sort direction, and that every companion
/// aggregate is compatible: same direction and (except for `any`) the same determining argument.
bool analyzeOrderByAggregate(
    const AggregateDescriptions & aggregates, const String & order_column, int sort_direction, OrderByAggregate & out)
{
    size_t order_idx = aggregates.size();
    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        if (aggregates[i].column_name == order_column)
        {
            order_idx = i;
            break;
        }
    }
    if (order_idx >= aggregates.size())
        return false;

    const auto & order_agg = aggregates[order_idx];
    auto order_info = order_agg.function->getTopKAggregateInfo();
    if (order_info.determined_by_first_row_direction == 0)
        return false;

    int required_direction = order_info.determined_by_first_row_direction;
    if (required_direction != INT_MAX && required_direction != sort_direction)
        return false;
    if (required_direction == INT_MAX)
        required_direction = sort_direction;

    const auto & determining_arg = order_agg.argument_names.back();
    for (const auto & agg : aggregates)
    {
        auto info = agg.function->getTopKAggregateInfo();
        if (info.determined_by_first_row_direction == 0)
            return false;
        if (info.determined_by_first_row_direction != INT_MAX && info.determined_by_first_row_direction != required_direction)
            return false;
        /// Non-`any` companions must read the same determining argument; otherwise first-row-per-group
        /// would produce wrong results (e.g. argMax(payload, v) alongside max(ts)).
        if (info.determined_by_first_row_direction != INT_MAX && agg.argument_names.back() != determining_arg)
            return false;
    }

    out.required_direction = required_direction;
    out.output_ordered_by_sort_key = order_info.output_ordered_by_sort_key;
    out.determining_arg = determining_arg;
    return true;
}

/// Minimum NDV across the GROUP BY columns from `uniq` statistics, or nullopt if statistics are
/// unavailable for any key. The minimum single-column NDV is a conservative lower bound on the
/// composite-key cardinality.
std::optional<UInt64> estimateMinGroupByNdv(ReadFromMergeTree & read_from_mt, const Names & keys, QueryPlan::Node * agg_child)
{
    /// GROUP BY keys may be qualified as `__table1.col`; resolve to storage column names.
    Names resolved_keys;
    resolved_keys.reserve(keys.size());
    for (const auto & key_name : keys)
        resolved_keys.push_back(resolveOriginalArgName(key_name, agg_child));

    auto estimator = read_from_mt.getConditionSelectivityEstimator(resolved_keys);
    if (!estimator)
        return {};

    auto profile = estimator->estimateRelationProfile();
    UInt64 min_ndv = std::numeric_limits<UInt64>::max();
    for (const auto & key_name : resolved_keys)
    {
        auto it = profile.column_stats.find(key_name);
        if (it == profile.column_stats.end() || it->second.num_distinct_values == 0)
            return {};
        min_ndv = std::min(min_ndv, it->second.num_distinct_values);
    }
    return min_ndv;
}

/// Cardinality gate. Early termination only pays for the sorted single-stream read when LIMIT is
/// small relative to the GROUP BY cardinality. With statistics, require `limit <= ndv * ratio`;
/// without, fall back to an absolute `limit <= max_limit` cap. Either path is disabled by setting
/// its knob to 0.
bool passesCardinalityGate(size_t limit, std::optional<UInt64> ndv, Float64 max_ndv_ratio, UInt64 max_limit)
{
    if (ndv.has_value())
        return max_ndv_ratio > 0.0 && static_cast<Float64>(limit) <= static_cast<Float64>(*ndv) * max_ndv_ratio;
    return max_limit != 0 && limit <= max_limit;
}

/// Whether the input can be read in physical sort order for early termination, and request it.
/// Rejects shapes where physical order may not match the query ORDER BY (so early termination could
/// drop correct groups): requires output_ordered_by_sort_key (min/max only), no collation, the
/// determining argument as the first sorting-key column, ascending (not reverse-flagged), and a
/// non-nullable, non-floating-point argument. requestReadingInOrder mutates the read step, so it
/// is called last.
bool requestSortedInput(
    ReadFromMergeTree & read_from_mt, const OrderByAggregate & order_agg, const String & order_arg_name,
    bool has_collator, size_t limit)
{
    if (!order_agg.output_ordered_by_sort_key || has_collator)
        return false;

    const auto & sorting_key = read_from_mt.getStorageMetadata()->getSortingKey();
    const auto & sorting_key_columns = sorting_key.column_names;
    bool first_key_reversed = !sorting_key.reverse_flags.empty() && sorting_key.reverse_flags[0];
    if (sorting_key_columns.empty() || sorting_key_columns[0] != order_arg_name || first_key_reversed)
        return false;

    /// NULL and floating-point NaN do not sort consistently with min/max, so the first row in
    /// physical order need not decide the group's aggregate: NaN sorts at the end of an ascending
    /// key (read first under DESC), so `ORDER BY max(x) DESC LIMIT K` could emit NaN where the
    /// standard pipeline skips it. Reject both determining-column types and fall back to the
    /// standard pipeline (mirrors the read-in-order optimization's guard).
    const auto & mt_header = *read_from_mt.getOutputHeader();
    if (mt_header.has(order_arg_name))
    {
        const auto & arg_type = mt_header.getByName(order_arg_name).type;
        if (isNullableOrLowCardinalityNullable(arg_type) || isFloat(*removeLowCardinalityAndNullable(arg_type)))
            return false;
    }

    return read_from_mt.requestReadingInOrder(1, order_agg.required_direction, limit);
}

}

/** Fuse `GROUP BY ... ORDER BY aggregate LIMIT K` into a single TopNAggregatingStep when the
  * input is physically sorted by the ORDER BY aggregate's argument (Mode 1): read in order and
  * stop after K distinct groups, since the first row of each group decides its aggregate.
  *
  * Matches LimitStep -> SortingStep(single column) -> [ExpressionStep] -> AggregatingStep where
  * the ORDER BY column is one aggregate determined by the first row in sort order (min/max, with
  * any/argMin/argMax companions on the same argument).
  *
  * TODO(Mode 2): an unsorted-input path (parallel partial top-K + threshold pruning) ships in a
  * follow-up PR; this handles Mode 1 only.
  */
void optimizeTopNAggregation(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Defer to forced projection. With force_optimize_projection / force_optimize_projection_name the
    /// user requires an aggregate projection to answer the query. This rewrite runs at the LimitStep,
    /// before projection selection reaches the child AggregatingStep, so firing it on the base-table
    /// plan would leave the forced projection unused and later raise PROJECTION_NOT_USED. Skip it.
    if (optimization_settings.force_use_projection || !optimization_settings.force_projection_name.empty())
        return;

    PlanMatch match;
    if (!matchPlan(node, match))
        return;

    const auto & params = match.aggregating_step->getParams();

    /// Trace the ORDER BY column through the optional expression to the aggregate output name.
    String order_column = match.sort_column.column_name;
    if (match.expr_step)
    {
        order_column = traceColumnThroughDAG(match.expr_step->getExpression(), order_column);
        if (order_column.empty())
            return;
    }

    OrderByAggregate order_agg;
    if (!analyzeOrderByAggregate(params.aggregates, order_column, match.sort_column.direction, order_agg))
        return;

    auto * agg_child = match.agg_node->children.size() == 1 ? match.agg_node->children[0] : nullptr;
    auto * read_from_mt = agg_child ? findReadFromMergeTree(*agg_child) : nullptr;
    if (!read_from_mt)
        return;

    if (!passesCardinalityGate(
            match.limit,
            estimateMinGroupByNdv(*read_from_mt, params.keys, agg_child),
            static_cast<Float64>(optimization_settings.topn_aggregation_max_ndv_ratio),
            optimization_settings.topn_aggregation_max_limit))
        return;

    String order_arg_name = resolveOriginalArgName(order_agg.determining_arg, agg_child);
    if (order_arg_name.empty())  /// argument is a computed expression, not a pass-through of a storage column
        return;
    if (!requestSortedInput(*read_from_mt, order_agg, order_arg_name, match.sort_column.collator != nullptr, match.limit))
        return;

    SortDescription topn_sort_desc;
    SortColumnDescription order_column_desc = match.sort_column;
    order_column_desc.column_name = order_column;
    topn_sort_desc.push_back(std::move(order_column_desc));

    /// Disable external aggregation for the Mode-1 Aggregator: it is bounded-memory (holds ~K
    /// groups) and the transform reads only in-memory data, so a spill would flush/reset groups it
    /// never reads. (Spill is on by default; max_memory_usage still bounds it.)
    auto topn_params = params;
    topn_params.max_bytes_before_external_group_by = 0;

    auto topn_step = std::make_unique<TopNAggregatingStep>(
        match.agg_node->step->getInputHeaders().front(),
        std::move(topn_params),
        topn_sort_desc,
        match.limit,
        order_agg.determining_arg);

    /// Insert TopNAggregating in place of the Sorting + Aggregating steps but KEEP the LimitStep on
    /// top (it is a no-op over the <= K already-limited rows). This preserves the LimitTransform that
    /// feeds `rows_before_limit_at_least`; without it the field would be dropped for these queries.
    auto & topn_node = nodes.emplace_back();
    topn_node.step = std::move(topn_step);
    topn_node.children = match.agg_node->children;

    if (match.expr_node)
    {
        match.expr_node->children = {&topn_node};
        node.children = {match.expr_node};
    }
    else
    {
        node.children = {&topn_node};
    }
}

}
