#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>
#include <ranges>

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
                | std::views::transform([](const auto & p) { return fmt::format("{}: {}", p.first, p.second.num_distinct_values); }),
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

        UInt64 key_number_of_distinct_values = key_stats->second.num_distinct_values;

        if (input_stats.estimated_rows)
            key_number_of_distinct_values = std::min(key_number_of_distinct_values, *input_stats.estimated_rows);

        aggregation_stats.column_stats[key].num_distinct_values = key_number_of_distinct_values;

        /// For now assume that aggregation columns are independent, so multiply their NDVs
        if (total_number_of_distinct_values)
            *total_number_of_distinct_values *= static_cast<Float64>(key_number_of_distinct_values);
    }

    if (total_number_of_distinct_values && input_stats.estimated_rows)
        total_number_of_distinct_values = std::min(*total_number_of_distinct_values, Float64(*input_stats.estimated_rows));
    else
        total_number_of_distinct_values = input_stats.estimated_rows;

    aggregation_stats.estimated_rows = total_number_of_distinct_values;

    return aggregation_stats;
}

/// Rows dropped by a limit are not a value-uniform sample (e.g. a TopN keeps one end of the
/// sorted range), so the child's value ranges and NULL fraction do not describe the output.
/// Applied whenever a limit is present: the row estimate cannot prove the limit does not
/// truncate (e.g. a TopN read is already scaled down by its `__topKFilter` prewhere).
void clearColumnValueRanges(std::unordered_map<String, ColumnStats> & column_stats)
{
    for (auto & [_, stats] : column_stats)
    {
        stats.min_value.reset();
        stats.max_value.reset();
        stats.null_fraction.reset();
    }
}

QueryPlan::Node * resolveSubplanReference(const QueryPlan::Node & node)
{
    if (const auto * reference = typeid_cast<const CommonSubplanReferenceStep *>(node.step.get()))
        return reference->getSubplanReferenceRoot();
    return nullptr;
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

class RelationStatsCacheSession;

namespace
{

RelationStats estimateReadRowsCountUncached(
    RelationStatsCacheSession & context,
    QueryPlan::Node & node,
    const ActionsDAG::Node * filter);

}

class RelationStatsCacheSession
{
public:
    RelationStatsCacheSession(RelationStatsCache & cache_, RelationStatsOptions options_)
        : cache(cache_)
        , options(options_)
        , invocation(++cache.next_invocation)
    {
        if (invocation == 0)
        {
            /// Keep zero reserved for persistent entries if the pass somehow performs 2^64 estimations.
            for (auto & entries : cache.entries_by_mode)
                std::erase_if(entries, [](const auto & entry) { return entry.second.invocation != 0; });
            invocation = ++cache.next_invocation;
        }
    }

    RelationStats estimate(QueryPlan::Node & node, const ActionsDAG::Node * filter)
    {
        if (const auto * entry = findCached(node, filter))
        {
            inherit(entry->options_independent, entry->invocation == 0);
            return entry->stats;
        }

        /// Recursive calls fold their cacheability into this node through `active_derivation`, keeping
        /// the existing estimator body free to return RelationStats directly.
        Derivation derivation;
        Derivation * parent_derivation = active_derivation;
        active_derivation = &derivation;
        RelationStats stats = estimateReadRowsCountUncached(*this, node, filter);
        active_derivation = parent_derivation;

        store(node, filter, stats, derivation);
        inherit(derivation.options_independent, derivation.persistent);
        return stats;
    }

    const RelationStatsOptions & getOptions() const { return options; }

    void markOptionsDependent() { active_derivation->options_independent = false; }
    void markRequestLocal() { active_derivation->persistent = false; }

private:
    struct Derivation
    {
        bool options_independent = true;
        bool persistent = true;
    };

    size_t modeIndex() const { return options.propagate_join_estimates ? 1 : 0; }

    const RelationStatsCache::Entry * findCached(const QueryPlan::Node & node, const ActionsDAG::Node * filter)
    {
        auto & entries = cache.entries_by_mode[modeIndex()];
        auto it = entries.find(&node);
        if (it == entries.end())
            return nullptr;

        const auto * referenced_subplan = resolveSubplanReference(node);
        const bool children_match = referenced_subplan
            ? it->second.children.size() == 1 && it->second.children.front() == referenced_subplan
            : it->second.children.size() == node.children.size() && std::ranges::equal(it->second.children, node.children);
        if (it->second.step != node.step.get() || !children_match)
        {
#if defined(DEBUG_OR_SANITIZER_BUILD)
            chassert(false, "RelationStatsCache entry was not invalidated after replacing a plan step or child");
#endif
            entries.erase(it);
            return nullptr;
        }

        if (it->second.filter != filter || (it->second.invocation != 0 && it->second.invocation != invocation))
            return nullptr;
        return &it->second;
    }

    void store(const QueryPlan::Node & node, const ActionsDAG::Node * filter, const RelationStats & stats, const Derivation & derivation)
    {
        RelationStatsCache::Entry entry{
            .stats = stats,
            .filter = filter,
            .step = node.step.get(),
            .children = {},
            .options_independent = derivation.options_independent,
            .invocation = derivation.persistent ? 0 : invocation,
        };
        if (const auto * referenced_subplan = resolveSubplanReference(node))
            entry.children.push_back(referenced_subplan);
        else
            entry.children.assign(node.children.begin(), node.children.end());

        cache.entries_by_mode[modeIndex()].insert_or_assign(&node, entry);
        if (derivation.options_independent)
        {
            const size_t other_mode = 1 - modeIndex();
            cache.entries_by_mode[other_mode].insert_or_assign(&node, std::move(entry));
        }
    }

    /// Fold a child's cacheability into the node currently being derived.
    void inherit(bool options_independent, bool persistent)
    {
        if (!active_derivation)
            return;
        active_derivation->options_independent &= options_independent;
        active_derivation->persistent &= persistent;
    }

    RelationStatsCache & cache;
    RelationStatsOptions options;
    UInt64 invocation;
    Derivation * active_derivation = nullptr;
};

namespace
{

RelationStats estimateReadRowsCountUncached(
    RelationStatsCacheSession & context,
    QueryPlan::Node & node,
    const ActionsDAG::Node * filter)
{
    IQueryPlanStep * step = node.step.get();
    const auto * logical_join = typeid_cast<const JoinStepLogical *>(step);
    QueryPlan::Node * referenced_subplan = resolveSubplanReference(node);
    if (logical_join || referenced_subplan)
        context.markRequestLocal();
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
            return RelationStats{.estimated_rows = 0, .table_name = table_display_name};

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

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto prewhere_info = reading->getPrewhereInfo();
                const ActionsDAG::Node * prewhere_node = prewhere_info
                    ? static_cast<const ActionsDAG::Node *>(
                          prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                    : nullptr;
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), filter, prewhere_node);
                RelationStats stats{
                    .estimated_rows = relation_profile.rows,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .source = RowEstimateSource::Statistics};
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpStatsForLogs(stats));
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
        bool has_filter = filter || reading->getPrewhereInfo();

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
        /// system.one always produces exactly one row — used to implement constant SELECTs like `SELECT 1`.
        return RelationStats{.estimated_rows = 1, .table_name = "system.one"};
    }

    if (logical_join && logical_join->isOptimized())
    {
        context.markOptionsDependent();
        if (!context.getOptions().propagate_join_estimates)
            return {};

        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        return RelationStats{
            .estimated_rows = logical_join->getResultRowsEstimation(),
            .column_stats = logical_join->getResultColumnStats(),
            .table_name = logical_join->getReadableRelationName(),
            .imprecise_estimate = logical_join->hasImpreciseEstimate()};
    }

    if (referenced_subplan)
        return context.estimate(*referenced_subplan, filter);

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = context.estimate(*node.children.front(), filter);
        auto limit = limit_step->getLimit();
        if (!estimated.estimated_rows || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        clearColumnValueRanges(estimated.column_stats);
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step);
        expression_step && !expression_step->getExpression().hasArrayJoin())
    {
        auto stats = context.estimate(*node.children.front(), filter);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto stats = context.estimate(*node.children.front(), predicate);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = context.estimate(*node.children.front(), filter);
        auto aggregation_stats = estimateAggregatingStepStats(*aggregating_step, stats);
        return aggregation_stats;
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = context.estimate(*node.children.front(), filter);
        if (sorting_step->getLimit())
        {
            if (!stats.estimated_rows || stats.estimated_rows > sorting_step->getLimit())
                stats.estimated_rows = sorting_step->getLimit();
            clearColumnValueRanges(stats.column_stats);
        }
        return stats;
    }

    /// Estimates must see through exchanges: they do not change row counts, and an
    /// already-distributed subtree would otherwise report unknown cardinality, degrading
    /// broadcast-vs-shuffle and join order decisions.
    if (dynamic_cast<LogicalExchangeStep *>(step))
        return context.estimate(*node.children.front(), filter);

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return context.estimate(*node.children.front(), filter);

    return {};
}

}

RelationStats
estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, RelationStatsOptions options, RelationStatsCache * cache)
{
    RelationStatsCache local_cache;
    RelationStatsCacheSession session(cache ? *cache : local_cache, options);
    return session.estimate(node, filter);
}

}
}
