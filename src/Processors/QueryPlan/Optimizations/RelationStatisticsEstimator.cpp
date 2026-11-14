#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <algorithm>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/StreamInQueryResultCacheStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapSnapshot.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool use_statistics;
}

namespace QueryPlanOptimizations
{

namespace
{

/// The sizes of the arrays are unknown, so an array join is assumed to produce this many rows per input row.
constexpr Float64 array_join_rows_multiplier = 100;

RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    std::optional<Float64> total_number_of_distinct_values = 1;
    RelationStats aggregation_stats;
    /// Carry imprecision and source from the input, or the annotation is lost for aggregation subqueries.
    aggregation_stats.imprecise_estimate = input_stats.imprecise_estimate;
    aggregation_stats.source = input_stats.source;
    for (const auto & key : aggregator_params.keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        if (key_stats == input_stats.column_stats.end())
        {
            /// Cannot calculate total number of groups if we don't know NDV of any of the aggregation columns.
            /// The estimate then falls back to the input row count (an over-count of groups), so it is no longer
            /// precise. Flag it and surface a missing-statistics source so the EXPLAIN label and the
            /// join-reordering diagnostic reflect that the fallback was caused by missing column statistics.
            total_number_of_distinct_values.reset();
            aggregation_stats.imprecise_estimate = true;
            if (aggregation_stats.source == RowEstimateSource::Statistics || aggregation_stats.source == RowEstimateSource::NoSource)
                aggregation_stats.source = RowEstimateSource::NoStatistics;
            continue;
        }

        ColumnStats grouping_key_stats = makeGroupingKeyStats(key_stats->second, input_stats);
        const UInt64 key_number_of_distinct_values = grouping_key_stats.num_distinct_values;
        aggregation_stats.column_stats.emplace(key, std::move(grouping_key_stats));

        /// For now assume that aggregation columns are independent, so multiply their NDVs
        if (total_number_of_distinct_values)
            *total_number_of_distinct_values *= static_cast<Float64>(key_number_of_distinct_values);
    }

    if (total_number_of_distinct_values && input_stats.estimated_rows)
        total_number_of_distinct_values = std::min(*total_number_of_distinct_values, Float64(*input_stats.estimated_rows));
    else
        total_number_of_distinct_values = input_stats.estimated_rows;

    if (total_number_of_distinct_values)
        aggregation_stats.estimated_rows = toUInt64Saturating(*total_number_of_distinct_values);

    /// Grouping sets and the overflow row of `WITH TOTALS` under `max_rows_to_group_by` add rows with default key
    /// values, which need not occur in the input.
    if (aggregating_step.isGroupingSets() || aggregator_params.overflow_row)
        aggregation_stats.addTransformation(Unmodeled);

    /// `max_rows_to_group_by` with overflow mode ANY keeps the first keys seen and BREAK stops reading:
    /// either way the output keys are a subset of the input's.
    if (aggregator_params.max_rows_to_group_by && aggregator_params.group_by_overflow_mode != OverflowMode::THROW)
        aggregation_stats.addTransformation(RowSubset);

    /// Grouping keys used to come without a value range. They keep one only while it encloses the output keys, which
    /// the annotations above decide, so this comes after them.
    keepOnlyEnclosingValueRanges(aggregation_stats.column_stats);
    return aggregation_stats;
}

/// Raises a read with rows to 1 row and 1 value per column: 1% of `=` and the 10% guess of distinct values truncate to 0
/// on a table of a few rows, and the joins and aggregations above multiply them.
void floorReadWithRows(RelationProfile & profile)
{
    profile.rows = std::max<UInt64>(profile.rows, 1);
    for (auto & [_, column_stats] : profile.column_stats)
        column_stats.num_distinct_values = std::max<UInt64>(column_stats.num_distinct_values, 1);
}

/// A filter on the elements or on the output of an array join leaves its row count unknown unless the input is empty.
void forgetFilteredArrayJoinRows(RelationStats & stats)
{
    if (!stats.estimated_rows || *stats.estimated_rows == 0)
        return;
    stats.estimated_rows.reset();
    stats.imprecise_estimate = true;
    stats.rows_exact = false;
}

/// Applies `LIMIT limit OFFSET offset` to the row count of `stats` and returns whether the limit may drop rows.
/// Only an exact input row count proves that the limit keeps every row, or gives the exact output row count.
bool applyRowLimit(RelationStats & stats, UInt64 limit, UInt64 offset, bool with_ties)
{
    if (const auto input_rows = stats.exactRows())
    {
        if (offset == 0 && *input_rows <= limit)
            return false;
        /// `WITH TIES` can keep more than `limit` rows.
        if (!with_ties)
        {
            stats.estimated_rows = std::min(*input_rows - std::min(*input_rows, offset), limit);
            return true;
        }
    }
    if (!stats.estimated_rows || *stats.estimated_rows > limit)
        stats.estimated_rows = limit;
    stats.rows_exact = false;
    return true;
}

/// How `estimateUnaryStepStats` derives the statistics of a step from those of its only input.
enum class UnaryStepStatsKind : UInt8
{
    /// The step is not estimated.
    Unsupported,
    Limit,
    Expression,
    /// `ArrayJoinStep` or an `ExpressionStep` with `arrayJoin`.
    ArrayJoin,
    Filter,
    Aggregating,
    Sorting,
    LogicalExchange,
    /// Passes its input rows and values through unchanged.
    PassThrough,
    /// Keeps the number of rows, but its effect on values is not modeled.
    UnmodeledRowPreserving,
};

UnaryStepStatsKind classifyUnaryStepStats(const IQueryPlanStep & step)
{
    if (typeid_cast<const LimitStep *>(&step))
        return UnaryStepStatsKind::Limit;
    if (typeid_cast<const ArrayJoinStep *>(&step))
        return UnaryStepStatsKind::ArrayJoin;

    /// `arrayJoin` changes the number of rows, so such an expression is estimated as an array join.
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(&step))
        return expression_step->getExpression().hasArrayJoin() ? UnaryStepStatsKind::ArrayJoin : UnaryStepStatsKind::Expression;

    if (typeid_cast<const FilterStep *>(&step))
        return UnaryStepStatsKind::Filter;
    if (typeid_cast<const AggregatingStep *>(&step))
        return UnaryStepStatsKind::Aggregating;
    if (typeid_cast<const SortingStep *>(&step))
        return UnaryStepStatsKind::Sorting;
    if (dynamic_cast<const LogicalExchangeStep *>(&step))
        return UnaryStepStatsKind::LogicalExchange;

    /// These steps leave the rows and the existing columns unchanged: they tee their input to several consumers, copy it
    /// into a buffer or the query result cache, build a runtime filter or extremes from it, or append window columns.
    if (typeid_cast<const CommonSubplanStep *>(&step) || typeid_cast<const SaveSubqueryResultToBufferStep *>(&step)
        || typeid_cast<const StreamInQueryResultCacheStep *>(&step) || typeid_cast<const BuildRuntimeFilterStep *>(&step)
        || typeid_cast<const ExtremesStep *>(&step) || typeid_cast<const WindowStep *>(&step))
        return UnaryStepStatsKind::PassThrough;

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(&step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return UnaryStepStatsKind::UnmodeledRowPreserving;
    return UnaryStepStatsKind::Unsupported;
}

/// A runtime filter prunes nothing at plan time, so it must not count as a filter when estimating.
/// True for a `__applyFilter` call or an always-true `x = x`, an `and` of such predicates, or an `or`
/// with at least one such operand. Aliases are looked through.
bool prunesNothingAtPlanTime(const ActionsDAG::Node * node)
{
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        return prunesNothingAtPlanTime(node->children.front());

    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return false;

    const auto & function_name = node->function_base->getName();
    if (function_name == "__applyFilter" || isAlwaysTrueEquality(*node))
        return true;

    if (node->children.empty())
        return false;

    if (function_name == "and")
        return std::ranges::all_of(node->children, prunesNothingAtPlanTime);

    /// One operand passes every row at plan time, so the whole `or` does. `addNullBypassForAntiJoin`
    /// builds this form for `LEFT ANTI JOIN`: `__applyFilter(...) OR isNull(key)`.
    if (function_name == "or")
        return std::ranges::any_of(node->children, prunesNothingAtPlanTime);

    return false;
}

/// The conditions on the rows of a read: the filter above it and the `PREWHERE`, as MergeTree estimates them, and the row policy.
std::vector<RPNBuilderTreeNode> getConditionsToEstimate(const SourceStepWithFilter & reading, const ActionsDAG::Node * filter) // STYLE_CHECK_ALLOW_STD_CONTAINERS -- `ConditionSelectivityEstimator::estimateRelationProfile` takes std::vector
{
    std::vector<RPNBuilderTreeNode> conditions; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- returned to `estimateRelationProfile`, which takes std::vector
    auto add_condition = [&](const ActionsDAG::Node * condition)
    {
        if (condition && !prunesNothingAtPlanTime(condition))
            conditions.emplace_back(condition, reading.getContext());
    };
    add_condition(filter);
    if (const auto & prewhere_info = reading.getPrewhereInfo())
        add_condition(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name));
    if (const auto & row_level_filter = reading.getRowLevelFilter())
        add_condition(row_level_filter->actions.tryFindInOutputs(row_level_filter->column_name));
    return conditions;
}

}

std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats)
{
    switch (classifyUnaryStepStats(step))
    {
        case UnaryStepStatsKind::Limit:
        {
            const auto & limit_step = static_cast<const LimitStep &>(step);
            if (applyRowLimit(input_stats, limit_step.getLimit(), limit_step.getOffset(), limit_step.withTies()))
                input_stats.addTransformation(RowSubset);
            /// A limit used to erase the value ranges, also when it cannot drop a row. It keeps only enclosing ones.
            keepOnlyEnclosingValueRanges(input_stats.column_stats);
            return input_stats;
        }
        case UnaryStepStatsKind::Expression:
        {
            const auto & expression_step = static_cast<const ExpressionStep &>(step);
            remapColumnStats(input_stats.column_stats, expression_step.getExpression());
            return input_stats;
        }
        case UnaryStepStatsKind::ArrayJoin: {
            /// Only an exactly empty input gives an exact output: no rows.
            const bool is_exactly_empty = input_stats.exactRows() == 0;
            const auto * array_join_step = typeid_cast<const ArrayJoinStep *>(&step);
            if (array_join_step && array_join_step->hasElementFilter())
                forgetFilteredArrayJoinRows(input_stats);
            else if (input_stats.estimated_rows)
                input_stats.estimated_rows = toUInt64Saturating(Float64(*input_stats.estimated_rows) * array_join_rows_multiplier);
            input_stats.rows_exact = is_exactly_empty;

            /// Array-joined columns lose their statistics.
            if (array_join_step)
                for (const auto & column : array_join_step->getColumns())
                    input_stats.column_stats.erase(column);
            else
                remapColumnStats(input_stats.column_stats, static_cast<const ExpressionStep &>(step).getExpression());

            /// The other columns keep their values, but each row repeats once per array element, and unless the join is
            /// LEFT, rows with empty arrays are dropped: the distinct set and range can only shrink, and the distribution
            /// follows the array sizes.
            input_stats.addTransformation(RowSubset);
            return input_stats;
        }
        case UnaryStepStatsKind::Filter:
        {
            const auto & filter_step = static_cast<const FilterStep &>(step);
            remapColumnStats(input_stats.column_stats, filter_step.getExpression());
            input_stats.addTransformation(RowSubset);
            input_stats.rows_exact = false;
            return input_stats;
        }
        case UnaryStepStatsKind::Aggregating:
            return estimateAggregatingStepStats(static_cast<const AggregatingStep &>(step), input_stats);
        case UnaryStepStatsKind::Sorting:
        {
            const auto & sorting_step = static_cast<const SortingStep &>(step);
            bool may_drop_rows
                = sorting_step.getLimit() && applyRowLimit(input_stats, sorting_step.getLimit(), /*offset=*/0, /*with_ties=*/false);
            /// With a non-throwing overflow mode, `max_rows_to_sort` and `max_bytes_to_sort` stop a sorting stream
            /// once it exceeds them, so even a sort without a limit can return an order-dependent part of its input.
            const auto & size_limits = sorting_step.getSettings().size_limits;
            if (size_limits.overflow_mode != OverflowMode::THROW && size_limits.hasLimits())
            {
                input_stats.rows_exact = false;
                may_drop_rows = true;
            }
            if (may_drop_rows)
                input_stats.addTransformation(RowSubset);
            /// A sort with a limit used to erase the value ranges, also when it cannot drop a row. It keeps only
            /// enclosing ones. A sort without a limit never erased them.
            if (sorting_step.getLimit())
                keepOnlyEnclosingValueRanges(input_stats.column_stats);
            return input_stats;
        }
        case UnaryStepStatsKind::LogicalExchange:
            /// Estimates must see through exchanges: they do not change rows or values, and an
            /// already-distributed subtree would otherwise report unknown cardinality, degrading
            /// broadcast-vs-shuffle and join order decisions. Runtime-filter transport sizes the
            /// exact phase from this estimate, and the build filter sits below the join shuffle, so
            /// the walk has to cross exchanges on every build.
            return input_stats;
        case UnaryStepStatsKind::PassThrough: return input_stats;
        case UnaryStepStatsKind::UnmodeledRowPreserving:
            /// Keeping the number of rows does not mean keeping the values. The steps known to pass values
            /// through are listed in `classifyUnaryStepStats`; anything else fails closed.
            input_stats.addTransformation(Unmodeled);
            return input_stats;
        case UnaryStepStatsKind::Unsupported:
            return std::nullopt;
    }
    UNREACHABLE();
}

namespace
{

/// Whether a read limit with a non-throwing overflow mode can end the read early instead of failing the
/// query. Row and byte limits, including the leaf ones, are checked against the progress of the whole query
/// and `max_execution_time` against the elapsed time, so the read can stop however few rows it selects.
/// These are the limits that the sources of the read enforce; leaf limits are set only on a leaf of a distributed
/// query (`distributed_depth > 0`).
bool mayStopReadingEarly(const SelectQueryInfo & query_info)
{
    if (!query_info.storage_limits)
        return false;
    return std::ranges::any_of(
        *query_info.storage_limits,
        [](const StorageLimits & limits)
        {
            const auto & local_limits = limits.local_limits;
            return (local_limits.size_limits.overflow_mode != OverflowMode::THROW && local_limits.size_limits.hasLimits())
                || (limits.leaf_limits.overflow_mode != OverflowMode::THROW && limits.leaf_limits.hasLimits())
                || (local_limits.timeout_overflow_mode != OverflowMode::THROW
                    && local_limits.speed_limits.max_execution_time.totalMicroseconds() != 0);
        });
}

/// What a read does to the rows and values that the statistics of its source describe. A filter step above the read
/// records its own row subset; it counts here only where the estimate of the read accounts for it.
struct ReadCoverage
{
    /// The read may return only some of the rows: it filters or limits them, skips deleted rows, or may stop early.
    bool may_drop_rows = false;
    /// The read may return values that the statistics did not measure.
    bool may_rewrite_values = false;
    /// The `n` of a top-K read (`ORDER BY ... LIMIT n`), see `mayDropRows`. The `__topKFilter` PREWHERE that a later
    /// optimization pass adds to such a read makes it drop rows as any predicate; the estimates for join planning
    /// are made before that.
    std::optional<UInt64> top_k_limit;

    /// Whether the read may return fewer rows than it selects. A top-K read keeps every row of the final top `n`:
    /// range analysis drops only granules and parts that cannot hold one, and a running boundary drops rows once `n`
    /// have been seen. So if nothing else drops rows, it keeps all `selected_rows` if there are at most `n`.
    bool mayDropRows(UInt64 selected_rows) const { return may_drop_rows || (top_k_limit && selected_rows > *top_k_limit); }

    /// Records in the column statistics of `stats` what the read may drop or rewrite.
    void recordIn(RelationStats & stats, UInt64 selected_rows) const
    {
        if (mayDropRows(selected_rows))
            stats.addTransformation(RowSubset);
        if (may_rewrite_values)
            stats.addTransformation(Unmodeled);
    }
};

bool hasLightweightDeletedRows(const RangesInDataParts & parts)
{
    return std::ranges::any_of(
        parts,
        [](const RangesInDataPart & part)
        { return part.data_part->hasLightweightDelete() || (part.parent_part && part.parent_part->hasLightweightDelete()); });
}

/// Whether a `UNIQUE KEY` read drops rows of `parts` superseded by later inserts. It drops them through the delete
/// bitmaps of its snapshot; they are not `_row_exists` deletes and leave no mutation, but the part statistics still
/// cover them. The bitmap of each part is checked, as the reader does: the analysis has already resolved them to drop
/// fully dead granules, and the snapshot memoizes them for the read.
bool hasUniqueKeyDeletedRows(const ReadFromMergeTree & reading, const RangesInDataParts & parts)
{
    const auto * delete_bitmap_snapshot = tryGetDeleteBitmapSnapshot(*reading.getStorageSnapshot());
    return delete_bitmap_snapshot
        && std::ranges::any_of(
               parts, [&](const RangesInDataPart & part) { return !delete_bitmap_snapshot->bitmapAt(part.data_part->info)->empty(); });
}

/// Whether a metadata-only `RENAME COLUMN` / `DROP COLUMN` is pending for some of `parts`. Until it is materialized, the
/// statistics of a part stay keyed by its old column names, while the reader resolves a renamed column through
/// `AlterConversions`: after `DROP COLUMN a, RENAME COLUMN b TO a` the statistics found for `a` describe the dropped
/// column, not the old `b`. The snapshot can keep such a mutation after every part has materialized it, so the commands
/// still pending for each part are checked, as the reader does. A projection part has a fake data version, so its
/// parent decides.
bool hasPendingMetadataMutations(const ReadFromMergeTree & reading, const RangesInDataParts & parts)
{
    const auto mutations_snapshot = reading.getMutationsSnapshot();
    if (!mutations_snapshot || !mutations_snapshot->hasMetadataMutations())
        return false;
    return std::ranges::any_of(
        parts,
        [&](const RangesInDataPart & part)
        {
            const auto & data_part = part.parent_part ? part.parent_part : part.data_part;
            return std::ranges::any_of(
                mutations_snapshot->getOnFlyMutationCommandsForPart(data_part),
                [](const MutationCommand & command) { return AlterConversions::isSupportedMetadataMutation(command.type); });
        });
}

/// Mutations and patch parts applied on the fly can rewrite values and delete rows.
bool hasOnTheFlyDataChanges(const ReadFromMergeTree & reading)
{
    const auto mutations_snapshot = reading.getMutationsSnapshot();
    return mutations_snapshot
        && (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasAlterMutations() || mutations_snapshot->hasPatchParts());
}

/// The coverage of a MergeTree read of `parts`. With `has_plan_time_filter`, the estimate accounts for a filter above
/// the read, so it describes a subset of the rows as well.
ReadCoverage analyzeReadCoverage(const ReadFromMergeTree & reading, const RangesInDataParts & parts, bool has_plan_time_filter)
{
    const auto & query_info = reading.getQueryInfo();
    const bool has_predicates = has_plan_time_filter || reading.getPrewhereInfo() || reading.getRowLevelFilter()
        || reading.getFilterActionsDAG() || reading.getDeferredPrewhereInfo() || reading.getDeferredRowLevelFilter();
    const bool has_row_selecting_modifiers = query_info.isFinal() || query_info.isStream() || query_info.trivial_limit
        || (query_info.table_expression_modifiers && query_info.table_expression_modifiers->hasSampleSizeRatio())
        || reading.getVectorSearchParameters().has_value();
    /// A transaction snapshot is deliberately absent: the visible parts are the exact input of the read.
    const bool has_deleted_rows = hasLightweightDeletedRows(parts) || hasUniqueKeyDeletedRows(reading, parts);
    const bool has_on_the_fly_data_changes = hasOnTheFlyDataChanges(reading);

    return ReadCoverage{
        .may_drop_rows = has_predicates || has_row_selecting_modifiers || has_deleted_rows || has_on_the_fly_data_changes
            || mayStopReadingEarly(query_info),
        /// `FINAL` can synthesize values in `SummingMergeTree`, `AggregatingMergeTree` and `GraphiteMergeTree`; every
        /// `FINAL` read counts, so that new merge modes fail closed.
        .may_rewrite_values = has_on_the_fly_data_changes || query_info.isFinal()
            || reading.getMergeTreeData().hasEnabledMaskingPolicies(reading.getContext()) || hasPendingMetadataMutations(reading, parts),
        .top_k_limit
        = reading.isSelectedForTopKFilterOptimization() ? std::optional<UInt64>(reading.getTopKFilterInfo()->limit_n) : std::nullopt,
    };
}

/// Statistics of a node that is estimated without first estimating a child: a read step or an
/// already optimized logical join. Returns `std::nullopt` for every other step.
std::optional<RelationStats>
estimateDirectRelationStats(QueryPlan::Node & node, const ActionsDAG::Node * filter, RelationStatsOptions options)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();

        /// Analyze partition and primary-key ranges before estimating the relation so column
        /// statistics come only from parts that can satisfy the query. Reuse the result for
        /// the index-based fallback below.
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResult();
        if (!analyzed_result)
        {
            /// Range analysis normally enforces throwing read limits and memoizes its result.
            /// At this stage, however, later planning may make the executed read exempt from those
            /// limits. In that case use an estimation-only analysis; execution will analyze again
            /// after its final read mode is known.
            analyzed_result = reading->hasThrowingReadRowLimit()
                ? reading->selectRangesToReadForEstimation(/*allow_query_condition_cache_=*/true)
                : reading->selectRangesToRead();
        }

        /// An exact empty range selection proves that the relation is empty. Other empty
        /// analysis results can be placeholders for deferred work, so only propagate zero
        /// when `has_exact_ranges` is set.
        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return RelationStats{.estimated_rows = 0, .table_name = table_display_name, .rows_exact = true};

        /// `STREAM` defers range analysis until execution. Its placeholder result has zero
        /// selected rows but does not mean that the relation is empty.
        if (reading->getQueryInfo().isStream() && analyzed_result && analyzed_result->selected_rows == 0)
        {
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        /// A runtime filter prunes nothing at plan time, so it does not count as a filter for the estimate.
        const ActionsDAG::Node * plan_time_filter = prunesNothingAtPlanTime(filter) ? nullptr : filter;

        /// A PREWHERE that prunes nothing at plan time is likewise not a plan-time filter.
        const PrewhereInfoPtr prewhere_info = reading->getPrewhereInfo();
        const ActionsDAG::Node * prewhere_node = prewhere_info
            ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
            : nullptr;
        const bool has_prewhere_filter = prewhere_info && !prunesNothingAtPlanTime(prewhere_node);
        if (!has_prewhere_filter)
            prewhere_node = nullptr;

        bool is_filtered_by_index = false;
        if (analyzed_result)
        {
            UInt64 total_parts = 0;
            UInt64 total_granules = 0;
            for (const auto & idx_stat : analyzed_result->index_stats)
            {
                /// We expect the first element to be an index with None type, which is used to estimate the total amount of data in the table.
                /// Further index_stats are used to estimate amount of filtered data after applying the index.
                if (ReadFromMergeTree::IndexType::None == idx_stat.type)
                {
                    total_parts = idx_stat.num_parts_after;
                    total_granules = idx_stat.num_granules_after;
                    continue;
                }

                is_filtered_by_index = is_filtered_by_index || (total_parts && idx_stat.num_parts_after < total_parts)
                    || (total_granules && idx_stat.num_granules_after < total_granules);

                if (is_filtered_by_index)
                    break;
            }
        }
        const bool has_filter = plan_time_filter || has_prewhere_filter;

        /// Runtime-filter transport may raise its hard exact-phase row cap to this estimate. Under an
        /// unindexed plan-time filter, column statistics still produce a number, but it is only a guess
        /// and would override the exact limit from the settings. So the transport gets no estimate and
        /// keeps the settings geometry. Join-order estimation keeps using statistics.
        if (options.for_runtime_filter_transport && has_filter && !is_filtered_by_index)
        {
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        const auto coverage = analyzeReadCoverage(
            *reading, analyzed_result ? analyzed_result->parts_with_ranges : reading->getParts(), plan_time_filter != nullptr);

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), plan_time_filter, prewhere_node);
                RelationStats stats{
                    .estimated_rows = relation_profile.rows,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .rows_exact = !coverage.mayDropRows(relation_profile.rows),
                    .source = RowEstimateSource::Statistics};
                coverage.recordIn(stats, relation_profile.rows);
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpRelationStatsForLogs(stats));
                return stats;
            }
        }
        if (auto stats_hint = parseTableStatsHint(reading->getContext(), table_display_name); !stats_hint.table_name.empty())
            return stats_hint;

        if (!analyzed_result)
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};

        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};

        return RelationStats{
            .estimated_rows = analyzed_result->selected_rows,
            .table_name = table_display_name,
            .imprecise_estimate = true,
            .rows_exact = !coverage.mayDropRows(analyzed_result->selected_rows),
            .source = RowEstimateSource::PrimaryIndex};
    }

    if (const auto * reading = typeid_cast<const ReadFromObjectStorageStep *>(step))
    {
        const auto estimate = reading->getOrCreateDataLakeReadEstimate();
        if (!estimate)
            return RelationStats{};

        /// No rows, as `RelationStats{}`, so the join order is the same; but the metadata was read, so it names the table
        /// and its source: `EXPLAIN` shows `t[no_stats~?]` and the data lake hint line lists it.
        RelationStats stats{
            .estimated_rows = {},
            .table_name = reading->getStorageID().getTableName(),
            .imprecise_estimate = true,
            .source = RowEstimateSource::DataLakeMetadata};
        if (!estimate->rows)
            return stats;
        const UInt64 rows = *estimate->rows;

        /// The manifest statistics describe every row of the remaining files, deleted rows included.
        const ReadCoverage coverage{
            .may_drop_rows = estimate->has_delete_files || reading->getFilterActionsDAG() || reading->getPrewhereInfo()
                || reading->getRowLevelFilter() || mayStopReadingEarly(reading->getQueryInfo()),
            .may_rewrite_values = false,
            .top_k_limit = std::nullopt};

        const auto conditions = getConditionsToEstimate(*reading, filter);
        if (conditions.empty() || rows == 0)
        {
            stats.estimated_rows = rows;
            if (estimate->estimator)
            {
                auto profile = estimate->estimator->estimateRelationProfile(reading->getStorageMetadata());
                if (rows != 0)
                    floorReadWithRows(profile);
                stats.column_stats = std::move(profile.column_stats);
                coverage.recordIn(stats, rows);
            }
            /// Without conditions, the rows of the remaining files, imprecise with delete files, also when they leave 0 rows.
            stats.imprecise_estimate = estimate->has_delete_files;
            return stats;
        }

        /// With `for_runtime_filter_transport` the rows size a sent runtime filter, so they must be an upper bound, not a guess.
        if (estimate->estimator && !options.for_runtime_filter_transport)
        {
            auto filtered = estimate->estimator->estimateRelationProfile(reading->getStorageMetadata(), conditions);
            floorReadWithRows(filtered);
            stats.estimated_rows = filtered.rows;
            stats.column_stats = std::move(filtered.column_stats);
            coverage.recordIn(stats, rows);
            return stats;
        }

        /// As for MergeTree without column statistics, conditions that pruned nothing give no estimate.
        if (!estimate->pruned_data_files)
            return stats;

        /// The remaining files bound the rows, as the selected parts bound a MergeTree read.
        stats.estimated_rows = rows;
        return stats;
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_display_name = reading->getStorage()->getName();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = table_display_name, .source = RowEstimateSource::Statistics};
    }

    /// We cannot do typeid_cast<const ReadFromSystemOneStep *>(step)
    /// since this is defined in clickhouse_storages_system module,
    /// which is not linked to current module
    if (step->getName() == "ReadFromSystemOne")
    {
        /// system.one always produces exactly one row - used to implement constant SELECTs like `SELECT 1`.
        return RelationStats{.estimated_rows = 1, .table_name = "system.one", .rows_exact = true};
    }

    /// Reads no rows, e.g. a `MergeTree` table without parts.
    if (typeid_cast<const ReadNothingStep *>(step))
        return RelationStats{.estimated_rows = 0, .table_name = {}, .rows_exact = true, .source = RowEstimateSource::Statistics};

    /// A join that has already been optimized enters its parent's join graph as a single relation,
    /// and the row count it settled on is the only estimate that relation has.
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        /// The name is already a chain of the sub-joins with their own estimates, so mark it composite
        /// to keep the parent from appending a second estimate to it.
        /// A plan-time filter above the join is not accounted for, so like a storage read with a filter
        /// it cannot use, the row count is then unknown. A runtime filter prunes nothing at plan time.
        const bool has_plan_time_filter = filter && !prunesNothingAtPlanTime(filter);
        auto stats = RelationStats{
            .estimated_rows = has_plan_time_filter ? std::nullopt : join_step->getResultRowsEstimation(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = has_plan_time_filter || join_step->hasImpreciseEstimate(),
            .composite = true};
        /// How a join transforms the column statistics of its inputs is not modeled.
        stats.addTransformation(Unmodeled);
        return stats;
    }

    return std::nullopt;
}

/// The statistics of the subtree rooted at `node` under `filter`, the predicate of a `FilterStep` above it, which the
/// estimate of a read or of a sub-join accounts for. The row estimate ignores a predicate that prunes nothing at plan
/// time (see `prunesNothingAtPlanTime`). The `FilterStep` itself records the row subset in the column statistics.
RelationStats estimateRelationStatsUnderFilter(QueryPlan::Node & node, const ActionsDAG::Node * filter, RelationStatsOptions options)
{
    if (auto direct_stats = estimateDirectRelationStats(node, filter, options))
        return std::move(*direct_stats);

    if (const auto * reference = typeid_cast<const CommonSubplanReferenceStep *>(node.step.get()))
        return estimateRelationStatsUnderFilter(*reference->getSubplanReferenceRoot(), filter, options);

    const auto kind = classifyUnaryStepStats(*node.step);
    /// Unsupported steps must stop before their children are visited: the result is unknown anyway.
    if (node.children.size() != 1 || kind == UnaryStepStatsKind::Unsupported)
        return {};

    /// A filter above an array join can read its output, which the statistics of the input do not describe.
    const ActionsDAG::Node * child_filter = kind == UnaryStepStatsKind::ArrayJoin ? nullptr : filter;
    if (const auto * filter_step = typeid_cast<const FilterStep *>(node.step.get()))
    {
        const auto & dag = filter_step->getExpression();
        child_filter = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
    }

    auto input_stats = estimateRelationStatsUnderFilter(*node.children.front(), child_filter, options);
    auto stats = estimateUnaryStepStats(*node.step, std::move(input_stats));
    if (stats && kind == UnaryStepStatsKind::ArrayJoin && filter)
        forgetFilteredArrayJoinRows(*stats);
    return std::move(stats).value_or(RelationStats{});
}

}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, RelationStatsOptions options)
{
    return estimateRelationStatsUnderFilter(node, /*filter=*/nullptr, options);
}
}
}
