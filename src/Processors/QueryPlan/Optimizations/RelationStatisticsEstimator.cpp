#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>
#include <ranges>
#include <vector>

#include <fmt/ranges.h>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 max_rows_to_read;
extern const SettingsUInt64 max_rows_to_read_leaf;
extern const SettingsOverflowMode read_overflow_mode;
extern const SettingsOverflowMode read_overflow_mode_leaf;
extern const SettingsBool use_statistics;
}

namespace QueryPlanOptimizations
{

std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats);

namespace
{

String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format(
        "{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? toString(stats.estimated_rows.value()) : "unknown",
        fmt::join(
            stats.column_stats
                | std::views::transform(
                    [](const auto & p)
                    {
                        return fmt::format(
                            "{}: {} (ndv: {}, range: {})",
                            p.first,
                            p.second.num_distinct_values,
                            p.second.ndv_provenance.toString(),
                            p.second.range_provenance.toString());
                    }),
            ", "));
}

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

    aggregation_stats.estimated_rows = total_number_of_distinct_values;

    if (aggregating_step.isGroupingSets() || aggregator_params.overflow_row)
        addTransformation(aggregation_stats.column_stats, Unsupported);

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
                if (!input_stats.estimated_rows || input_stats.estimated_rows > sorting_step.getLimit())
                    input_stats.estimated_rows = sorting_step.getLimit();
                if (may_truncate_rows)
                    addTransformation(input_stats.column_stats, NonUniformRowSubset);
            }
            return input_stats;
        }
        case UnaryStepStatsKind::LogicalExchange:
            /// Exchanges do not change rows or values.
            return input_stats;
        case UnaryStepStatsKind::PreservingTransform:
            /// Preserving row count alone does not prove that the key values are preserved. Keep the
            /// observed values for diagnostics and make consumers fail closed through provenance.
            addTransformation(input_stats.column_stats, Unsupported);
            return input_stats;
        case UnaryStepStatsKind::Unsupported:
            return std::nullopt;
    }
    UNREACHABLE();
}

namespace
{

struct DirectRelationStats
{
    RelationStats stats;
    bool options_independent = true;
    bool persistent = true;
};

DirectRelationStats persistentRelationStats(RelationStats stats)
{
    return {.stats = std::move(stats)};
}

std::optional<DirectRelationStats>
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
            const auto & settings = reading->getContext()->getSettingsRef();
            const bool has_throwing_row_limit
                = (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
                || (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf]);

            /// Range analysis normally enforces throwing read limits and memoizes its result.
            /// At this stage, however, later planning may make the executed read exempt from those
            /// limits. In that case use an estimation-only analysis; execution will analyze again
            /// after its final read mode is known.
            analyzed_result = has_throwing_row_limit ? reading->selectRangesToReadForEstimation() : reading->selectRangesToRead();
        }

        /// An exact empty range selection proves that the relation is empty. Other empty
        /// analysis results can be placeholders for deferred work, so only propagate zero
        /// when `has_exact_ranges` is set.
        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return persistentRelationStats(RelationStats{.estimated_rows = 0, .table_name = table_display_name, .rows_exact = true});

        /// `STREAM` defers range analysis until execution. Its placeholder result has zero
        /// selected rows but does not mean that the relation is empty.
        if (reading->getQueryInfo().isStream() && analyzed_result && analyzed_result->selected_rows == 0)
        {
            return persistentRelationStats(RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics});
        }

        auto prewhere_info = reading->getPrewhereInfo();
        const ActionsDAG::Node * prewhere_node = prewhere_info
            ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
            : nullptr;
        const auto & query_info = reading->getQueryInfo();
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
        const bool has_row_subset = filter || prewhere_info || reading->getRowLevelFilter() || reading->getFilterActionsDAG()
            || reading->getDeferredPrewhereInfo() || reading->getDeferredRowLevelFilter() || query_info.isFinal() || query_info.isStream()
            || query_info.trivial_limit || has_lightweight_deleted_rows || has_on_fly_data_changes || has_masking_policy
            || (query_info.table_expression_modifiers && query_info.table_expression_modifiers->hasSampleSizeRatio());

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
                    .rows_exact = !has_row_subset,
                    .source = RowEstimateSource::Statistics};
                if (has_row_subset)
                    addTransformation(stats.column_stats, RowSubset);
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpStatsForLogs(stats));
                return persistentRelationStats(std::move(stats));
            }
        }
        if (auto stats_hint = parseTableStatsHint(reading->getContext(), table_display_name); !stats_hint.table_name.empty())
            return persistentRelationStats(std::move(stats_hint));

        if (!analyzed_result)
            return persistentRelationStats(RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics});

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
            return persistentRelationStats(RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics});

        return persistentRelationStats(RelationStats{
            .estimated_rows = analyzed_result->selected_rows,
            .table_name = table_display_name,
            .imprecise_estimate = true,
            .rows_exact = !has_row_subset,
            .source = RowEstimateSource::PrimaryIndex});
    }

    if (typeid_cast<const ReadFromObjectStorageStep *>(step))
        return persistentRelationStats(RelationStats{});

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_display_name = reading->getStorage()->getName();
        return persistentRelationStats(
            RelationStats{.estimated_rows = estimated_rows, .table_name = table_display_name, .source = RowEstimateSource::Statistics});
    }

    /// We cannot do typeid_cast<const ReadFromSystemOneStep *>(step)
    /// since this is defined in clickhouse_storages_system module,
    /// which is not linked to current module
    if (step->getName() == "ReadFromSystemOne")
    {
        /// system.one always produces exactly one row - used to implement constant SELECTs like `SELECT 1`.
        return persistentRelationStats(RelationStats{.estimated_rows = 1, .table_name = "system.one", .rows_exact = true});
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// This is the only direct estimate whose result depends on `RelationStatsOptions`. Logical
        /// joins are mutable during this pass, so neither their entries nor ancestors derived from
        /// them may survive beyond this invocation.
        if (!options.propagate_join_estimates)
            return DirectRelationStats{RelationStats{}, false, false};

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
        return DirectRelationStats{std::move(stats), false, false};
    }

    return std::nullopt;
}

}

void RelationStatsCache::invalidate(const QueryPlan::Node & node)
{
    for (auto & entries : entries_by_mode)
        entries.erase(&node);
}

void RelationStatsCache::rebindNode(const QueryPlan::Node & old_node, const QueryPlan::Node & new_node)
{
    for (auto & entries : entries_by_mode)
    {
        auto entry = entries.extract(&old_node);
        if (entry.empty())
            continue;

        entries.erase(&new_node);
        entry.key() = &new_node;
        entries.insert(std::move(entry));
    }
}

RelationStats
estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, RelationStatsOptions options, RelationStatsCache * cache)
{
    RelationStatsCache local_cache;
    RelationStatsCache & stats_cache = cache ? *cache : local_cache;
    UInt64 invocation = ++stats_cache.next_invocation;
    if (invocation == 0)
    {
        /// Keep zero reserved for persistent entries if the pass somehow performs 2^64 estimations.
        for (auto & entries : stats_cache.entries_by_mode)
            std::erase_if(entries, [](const auto & entry) { return entry.second.invocation != 0; });
        invocation = ++stats_cache.next_invocation;
    }

    const auto mode_index
        = [](RelationStatsOptions current_options) { return static_cast<size_t>(current_options.propagate_join_estimates); };

    const auto find_cached
        = [&](const QueryPlan::Node & current_node, const ActionsDAG::Node * current_filter) -> const RelationStatsCache::Entry *
    {
        auto & entries = stats_cache.entries_by_mode[mode_index(options)];
        auto it = entries.find(&current_node);
        if (it == entries.end() || it->second.filter != current_filter
            || (it->second.invocation != 0 && it->second.invocation != invocation))
            return nullptr;
        return &it->second;
    };

    const auto store_cached = [&](const QueryPlan::Node & current_node,
                                  const ActionsDAG::Node * current_filter,
                                  RelationStats stats,
                                  bool options_independent,
                                  bool persistent)
    {
        RelationStatsCache::Entry entry{
            .stats = std::move(stats),
            .filter = current_filter,
            .options_independent = options_independent,
            .invocation = persistent ? 0 : invocation};
        stats_cache.entries_by_mode[mode_index(options)].insert_or_assign(&current_node, entry);

        if (options_independent)
        {
            const size_t other_mode = 1 - mode_index(options);
            stats_cache.entries_by_mode[other_mode].insert_or_assign(&current_node, std::move(entry));
        }
    };

    enum class FrameKind : UInt8
    {
        Unvisited,
        Unary,
        CommonSubplanReference,
    };

    struct Frame
    {
        QueryPlan::Node * node;
        const ActionsDAG::Node * filter;
        QueryPlan::Node * child = nullptr;
        const ActionsDAG::Node * child_filter = nullptr;
        FrameKind kind = FrameKind::Unvisited;
    };

    std::vector<Frame> stack;
    stack.push_back({.node = &node, .filter = filter});

    while (!stack.empty())
    {
        auto & frame = stack.back();
        if (find_cached(*frame.node, frame.filter))
        {
            stack.pop_back();
            continue;
        }

        if (frame.kind == FrameKind::Unvisited)
        {
            if (auto direct_stats = estimateDirectRelationStats(*frame.node, frame.filter, options))
            {
                store_cached(
                    *frame.node,
                    frame.filter,
                    std::move(direct_stats->stats),
                    direct_stats->options_independent,
                    direct_stats->persistent);
                stack.pop_back();
                continue;
            }

            if (const auto * reference = typeid_cast<const CommonSubplanReferenceStep *>(frame.node->step.get()))
            {
                frame.child = reference->getSubplanReferenceRoot();
                frame.child_filter = frame.filter;
                frame.kind = FrameKind::CommonSubplanReference;
            }
            else
            {
                /// Unsupported steps must stop before their children are visited. Besides failing closed,
                /// this keeps estimation independent of the depth below an unsupported boundary.
                if (frame.node->children.size() != 1 || classifyUnaryStepStats(*frame.node->step) == UnaryStepStatsKind::Unsupported)
                {
                    /// An unoptimized logical join can be optimized or converted later in this pass.
                    /// Do not persist its empty result, or any ancestor derived from it, across calls.
                    const bool persistent = !typeid_cast<const JoinStepLogical *>(frame.node->step.get());
                    store_cached(*frame.node, frame.filter, {}, true, persistent);
                    stack.pop_back();
                    continue;
                }

                frame.child = frame.node->children.front();
                frame.child_filter = frame.filter;
                if (const auto * filter_step = typeid_cast<const FilterStep *>(frame.node->step.get()))
                {
                    const auto & dag = filter_step->getExpression();
                    frame.child_filter = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
                }
                frame.kind = FrameKind::Unary;
            }

            if (!find_cached(*frame.child, frame.child_filter))
            {
                auto * child = frame.child;
                const auto * child_filter = frame.child_filter;
                stack.push_back({.node = child, .filter = child_filter});
                continue;
            }
        }

        const auto * child_entry = find_cached(*frame.child, frame.child_filter);
        if (!child_entry)
            return {};

        RelationStats stats = child_entry->stats;
        if (frame.kind == FrameKind::Unary)
            stats = estimateUnaryStepStats(*frame.node->step, std::move(stats)).value_or(RelationStats{});

        store_cached(
            *frame.node,
            frame.filter,
            std::move(stats),
            child_entry->options_independent,
            child_entry->invocation == 0 && frame.kind != FrameKind::CommonSubplanReference);
        stack.pop_back();
    }

    const auto * result = find_cached(node, filter);
    if (!result)
        return {};
    return result->stats;
}
}
}
