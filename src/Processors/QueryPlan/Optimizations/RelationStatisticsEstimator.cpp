#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
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
#include <Processors/QueryPlan/SaveSubqueryResultToBufferStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/StreamInQueryResultCacheStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

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

        ColumnStats grouping_key_stats = QueryPlanOptimizations::makeGroupingKeyStats(
            key_stats->second, input_stats.estimated_rows, input_stats.rows_exact);
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

    if (aggregating_step.isGroupingSets() || aggregator_params.overflow_row)
        addTransformation(aggregation_stats.column_stats, Unsupported);

    /// `max_rows_to_group_by` with overflow mode ANY keeps the first keys seen and BREAK stops reading:
    /// either way the output key set is an order-dependent subset of the input's distinct keys.
    if (aggregator_params.max_rows_to_group_by && aggregator_params.group_by_overflow_mode != OverflowMode::THROW)
        addTransformation(aggregation_stats.column_stats, NonUniformRowSubset);

    return aggregation_stats;
}

enum class UnaryStepStatsKind : UInt8
{
    Unsupported,
    Limit,
    Expression,
    Filter,
    Aggregating,
    Sorting,
    LogicalExchange,
    ValuePreservingTransform,
    PreservingTransform,
};

UnaryStepStatsKind classifyUnaryStepStats(const IQueryPlanStep & step)
{
    if (typeid_cast<const LimitStep *>(&step))
        return UnaryStepStatsKind::Limit;

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(&step))
    {
        /// ARRAY JOIN changes the number of rows. Keep this explicit instead of relying on the
        /// generic transforming-step traits so the supported set cannot silently widen.
        return expression_step->getExpression().hasArrayJoin()
            ? UnaryStepStatsKind::Unsupported
            : UnaryStepStatsKind::Expression;
    }

    if (typeid_cast<const FilterStep *>(&step))
        return UnaryStepStatsKind::Filter;
    if (typeid_cast<const AggregatingStep *>(&step))
        return UnaryStepStatsKind::Aggregating;
    if (typeid_cast<const SortingStep *>(&step))
        return UnaryStepStatsKind::Sorting;
    if (dynamic_cast<const LogicalExchangeStep *>(&step))
        return UnaryStepStatsKind::LogicalExchange;

    /// Tees the same rows and columns to multiple consumers.
    if (typeid_cast<const CommonSubplanStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;
    /// Copies selected columns to a buffer while passing the input through unchanged.
    if (typeid_cast<const SaveSubqueryResultToBufferStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;
    /// Adds an extremes port without changing the main stream.
    if (typeid_cast<const ExtremesStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;
    /// Appends window result columns while leaving existing columns unchanged.
    if (typeid_cast<const WindowStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;
    /// Builds a runtime filter while passing the input through unchanged.
    if (typeid_cast<const BuildRuntimeFilterStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;
    /// Writes to the query-result cache while passing the input through unchanged.
    if (typeid_cast<const StreamInQueryResultCacheStep *>(&step))
        return UnaryStepStatsKind::ValuePreservingTransform;

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(&step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return UnaryStepStatsKind::PreservingTransform;
    return UnaryStepStatsKind::Unsupported;
}

}

std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats)
{
    switch (classifyUnaryStepStats(step))
    {
        case UnaryStepStatsKind::Limit:
        {
            const auto & limit_step = static_cast<const LimitStep &>(step);
            const auto limit = limit_step.getLimit();
            const auto offset = limit_step.getOffset();
            const bool preserves_all_rows = input_stats.rows_exact && input_stats.estimated_rows && offset == 0
                && *input_stats.estimated_rows <= limit;
            if (preserves_all_rows)
                return input_stats;

            if (input_stats.rows_exact && input_stats.estimated_rows && !limit_step.withTies())
            {
                const UInt64 rows_after_offset = *input_stats.estimated_rows > offset ? *input_stats.estimated_rows - offset : 0;
                input_stats.estimated_rows = std::min<UInt64>(rows_after_offset, limit);
            }
            else
            {
                if (!input_stats.estimated_rows || input_stats.estimated_rows > limit)
                    input_stats.estimated_rows = limit;
                input_stats.rows_exact = false;
            }
            addTransformation(input_stats.column_stats, NonUniformRowSubset);
            return input_stats;
        }
        case UnaryStepStatsKind::Expression:
        {
            const auto & expression_step = static_cast<const ExpressionStep &>(step);
            remapColumnStats(input_stats.column_stats, expression_step.getExpression());
            return input_stats;
        }
        case UnaryStepStatsKind::Filter:
        {
            const auto & filter_step = static_cast<const FilterStep &>(step);
            remapColumnStats(input_stats.column_stats, filter_step.getExpression());
            addTransformation(input_stats.column_stats, RowSubset);
            input_stats.rows_exact = false;
            return input_stats;
        }
        case UnaryStepStatsKind::Aggregating:
            return estimateAggregatingStepStats(static_cast<const AggregatingStep &>(step), input_stats);
        case UnaryStepStatsKind::Sorting:
        {
            const auto & sorting_step = static_cast<const SortingStep &>(step);
            if (sorting_step.getLimit())
            {
                const bool may_truncate_rows = !input_stats.rows_exact || !input_stats.estimated_rows
                    || *input_stats.estimated_rows > sorting_step.getLimit();
                /// A synthesized limit is not an exact row count when the input count is unknown.
                if (!input_stats.estimated_rows)
                    input_stats.rows_exact = false;
                if (!input_stats.estimated_rows || input_stats.estimated_rows > sorting_step.getLimit())
                    input_stats.estimated_rows = sorting_step.getLimit();
                if (may_truncate_rows)
                    addTransformation(input_stats.column_stats, NonUniformRowSubset);
            }
            return input_stats;
        }
        case UnaryStepStatsKind::LogicalExchange:
            /// Estimates must see through exchanges: they do not change rows or values, and an
            /// already-distributed subtree would otherwise report unknown cardinality, degrading
            /// broadcast-vs-shuffle and join order decisions.
            return input_stats;
        case UnaryStepStatsKind::ValuePreservingTransform:
            return input_stats;
        case UnaryStepStatsKind::PreservingTransform:
            /// Preserving row count alone does not prove that the key values are preserved. Known
            /// value-preserving steps are allow-listed in `classifyUnaryStepStats`; anything else
            /// fails closed. Keep observed values for diagnostics and record that they are unsupported.
            addTransformation(input_stats.column_stats, Unsupported);
            return input_stats;
        case UnaryStepStatsKind::Unsupported:
            return std::nullopt;
    }
    UNREACHABLE();
}

namespace
{

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

        auto prewhere_info = reading->getPrewhereInfo();
        const ActionsDAG::Node * prewhere_node = prewhere_info
            ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
            : nullptr;
        const auto & query_info = reading->getQueryInfo();
        const bool has_final = query_info.isFinal();
        const bool has_lightweight_deleted_rows
            = analyzed_result
            && std::ranges::any_of(
                  analyzed_result->parts_with_ranges,
                  [](const auto & part)
                  { return part.data_part->hasLightweightDelete() || (part.parent_part && part.parent_part->hasLightweightDelete()); });
        const auto mutations_snapshot = reading->getMutationsSnapshot();
        const bool has_on_fly_data_changes = mutations_snapshot
            && (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasAlterMutations() || mutations_snapshot->hasPatchParts());
        const bool has_masking_policy = reading->getMergeTreeData().hasEnabledMaskingPolicies(reading->getContext());
        /// These transformations can rewrite values, so the on-disk NDVs and ranges are no longer bounds.
        /// In particular, FINAL can synthesize values in SummingMergeTree, AggregatingMergeTree and
        /// GraphiteMergeTree. Treat every FINAL read conservatively so new merge modes fail closed.
        const bool has_unsupported_value_changes = has_on_fly_data_changes || has_masking_policy || has_final;
        /// Predicates cover explicit, deferred and row-policy filters; query modifiers cover `FINAL`,
        /// `SAMPLE`, stream reads and trivial limits; deleted rows, vector-index selection and the
        /// top-k skip-index threshold can prune rows without a filter step in the plan. A transaction
        /// snapshot is deliberately absent: the visible parts are the exact input for that read.
        const bool has_row_subset = filter || prewhere_info || reading->getRowLevelFilter() || reading->getFilterActionsDAG()
            || reading->getDeferredPrewhereInfo() || reading->getDeferredRowLevelFilter() || has_final || query_info.isStream()
            || query_info.trivial_limit || has_lightweight_deleted_rows || reading->getVectorSearchParameters().has_value()
            || reading->isSelectedForTopKFilterOptimization()
            || (query_info.table_expression_modifiers && query_info.table_expression_modifiers->hasSampleSizeRatio());
        const bool has_inexact_row_count = has_row_subset || has_on_fly_data_changes;

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), filter, prewhere_node);
                RelationStats stats{
                    .estimated_rows = relation_profile.rows,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .rows_exact = !has_inexact_row_count,
                    .source = RowEstimateSource::Statistics};
                if (has_row_subset)
                    addTransformation(stats.column_stats, RowSubset);
                if (has_unsupported_value_changes)
                    addTransformation(stats.column_stats, Unsupported);
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

        bool is_filtered_by_index = false;
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
        bool has_filter = filter || prewhere_info;

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
            .rows_exact = !has_inexact_row_count,
            .source = RowEstimateSource::PrimaryIndex};
    }

    if (typeid_cast<const ReadFromObjectStorageStep *>(step))
        return RelationStats{};

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

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// Without the explicit opt-in, keep the previous behavior: a sub-join reports unknown statistics.
        if (!options.propagate_join_estimates)
            return RelationStats{};

        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        auto stats = RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = join_step->hasImpreciseEstimate()};
        addTransformation(stats.column_stats, Unsupported);
        const auto strictness = join_step->getJoinOperator().strictness;
        if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
            addTransformation(stats.column_stats, RowSubset);
        return stats;
    }

    return std::nullopt;
}

}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, RelationStatsOptions options)
{
    if (auto direct_stats = estimateDirectRelationStats(node, filter, options))
        return std::move(*direct_stats);

    if (const auto * reference = typeid_cast<const CommonSubplanReferenceStep *>(node.step.get()))
        return estimateReadRowsCount(*reference->getSubplanReferenceRoot(), filter, options);

    /// Unsupported steps must stop before their children are visited: the result is unknown anyway.
    if (node.children.size() != 1 || classifyUnaryStepStats(*node.step) == UnaryStepStatsKind::Unsupported)
        return {};

    const ActionsDAG::Node * child_filter = filter;
    if (const auto * filter_step = typeid_cast<const FilterStep *>(node.step.get()))
    {
        const auto & dag = filter_step->getExpression();
        child_filter = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
    }

    auto input_stats = estimateReadRowsCount(*node.children.front(), child_filter, options);
    return estimateUnaryStepStats(*node.step, std::move(input_stats)).value_or(RelationStats{});
}

}
}
