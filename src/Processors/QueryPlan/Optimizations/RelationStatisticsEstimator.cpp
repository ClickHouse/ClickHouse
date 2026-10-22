#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>
#include <ranges>

#include <fmt/ranges.h>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinGraphStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/ReadFromCommonBufferStep.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool use_statistics;
extern const SettingsBool use_primary_key;
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

/// The conditions a conjunction is made of; any other condition is one conjunct.
void collectConjuncts(const ActionsDAG::Node * node, std::vector<const ActionsDAG::Node *> & conjuncts)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            collectConjuncts(child, conjuncts);
        return;
    }
    conjuncts.push_back(node);
}

/// How the key analysis represents a condition of a read.
enum class KeyCoverage : uint8_t
{
    /// The condition is estimated by the statistics.
    None,
    /// The partition key represents all of the condition: partition pruning already removed the parts
    /// it rejects, so the parts the statistics describe reflect it.
    Partition,
    /// The primary key represents all of the condition: the ranges the primary key analysis keeps
    /// for it measure it.
    PrimaryKey,
};

/// A condition counts only when the key analysis represents all of it: a part it cannot express
/// (the `v = 1` of `(k = 1 AND v = 1) OR k = 2`) would be lost otherwise.
KeyCoverage keyCoverage(const ActionsDAG::Node * conjunct, const ReadFromMergeTree & reading)
{
    const auto & metadata = reading.getStorageMetadata();
    const auto & context = reading.getContext();
    const ActionsDAGWithInversionPushDown dag(conjunct, context, /*boolean_context*/ true);
    const auto represents = [&](const KeyDescription & key, bool skip_analysis)
    {
        /// Only a set already built takes part: estimating must not run a subquery.
        const KeyCondition condition(dag, context, key.column_names, key.expression,
            /*single_point*/ false, skip_analysis, /*require_ready_sets*/ true);
        return !condition.anyUnknownOrAlwaysTrue();
    };
    if (metadata->hasPrimaryKey() && represents(metadata->getPrimaryKey(), !context->getSettingsRef()[Setting::use_primary_key]))
        return KeyCoverage::PrimaryKey;
    if (metadata->hasPartitionKey() && represents(metadata->getPartitionKey(), /*skip_analysis*/ false))
        return KeyCoverage::Partition;
    return KeyCoverage::None;
}

/// Share of the rows of `parts` in the ranges the primary key analysis keeps for `conjuncts` alone,
/// starting from whole parts: skip indexes, the query condition cache and the other conditions take no
/// part, so the statistics of the other conditions are not counted twice. The parts are the ones the
/// statistics describe, so the share applies to their rows.
std::optional<Float64> primaryKeyShare(
    const ReadFromMergeTree & reading, const RangesInDataParts & parts, const std::vector<const ActionsDAG::Node *> & conjuncts)
{
    const auto & metadata = reading.getStorageMetadata();
    const auto & context = reading.getContext();
    std::vector<KeyCondition> conditions;
    conditions.reserve(conjuncts.size());
    for (const auto * conjunct : conjuncts)
        conditions.emplace_back(ActionsDAGWithInversionPushDown(conjunct, context, /*boolean_context*/ true), context,
            metadata->getPrimaryKey().column_names, metadata->getPrimaryKey().expression,
            /*single_point*/ false, /*skip_analysis*/ false, /*require_ready_sets*/ true);

    UInt64 total_rows = 0;
    UInt64 kept_rows = 0;
    const auto log = getLogger("RelationStatisticsEstimator");
    for (const auto & part : parts)
    {
        const auto & data_part = part.data_part;
        MarkRanges ranges{MarkRange(0, data_part->index_granularity->getMarksCountWithoutFinal())};
        for (const auto & condition : conditions)
        {
            if (ranges.empty())
                break;
            ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                data_part, ranges, /*part_starting_offset_in_query*/ 0, metadata, condition,
                /*part_offset_condition*/ nullptr, /*total_offset_condition*/ nullptr, /*exact_ranges*/ nullptr,
                /*pk_to_minmax_slot*/ nullptr, context->getSettingsRef(), log);
        }
        total_rows += data_part->rows_count;
        kept_rows += data_part->index_granularity->getRowsCountInRanges(ranges);
    }
    if (total_rows == 0)
        return {};
    return std::min(1.0, static_cast<Float64>(kept_rows) / static_cast<Float64>(total_rows));
}

/// The statistics estimate of a read whose estimate took a default, with the conditions the key
/// analysis represents taken from that analysis instead: a default for a function of a sorting key
/// column (`toStartOfQuarter(date) IN (...)`) is then replaced by the rows of the ranges the primary
/// key keeps for it, and the statistics estimate the rest. The result stays marked as taken from
/// defaults: it is a better point estimate, not a measurement of the whole filter. Unset when no
/// condition is represented.
std::optional<RelationProfile> estimateWithKeyAnalysis(
    const ReadFromMergeTree & reading,
    const ConditionSelectivityEstimator & estimator,
    const StorageMetadataPtr & metadata,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const ActionsDAG::Node * filter,
    const ActionsDAG::Node * prewhere)
{
    std::vector<const ActionsDAG::Node *> prewhere_conjuncts;
    if (prewhere)
        collectConjuncts(prewhere, prewhere_conjuncts);
    std::unordered_set<String> prewhere_names;
    for (const auto * conjunct : prewhere_conjuncts)
        prewhere_names.insert(conjunct->result_name);

    std::vector<const ActionsDAG::Node *> conjuncts = prewhere_conjuncts;
    if (filter)
    {
        std::vector<const ActionsDAG::Node *> filter_conjuncts;
        collectConjuncts(filter, filter_conjuncts);
        /// A condition moved to `PREWHERE` stays in the filter as an input column; it is counted once.
        for (const auto * conjunct : filter_conjuncts)
            if (conjunct->type != ActionsDAG::ActionType::INPUT || !prewhere_names.contains(conjunct->result_name))
                conjuncts.push_back(conjunct);
    }

    bool any_represented = false;
    std::vector<const ActionsDAG::Node *> primary_key_conjuncts;
    std::vector<RPNBuilderTreeNode> rest;
    for (const auto * conjunct : conjuncts)
    {
        switch (keyCoverage(conjunct, reading))
        {
            case KeyCoverage::PrimaryKey:
                primary_key_conjuncts.push_back(conjunct);
                any_represented = true;
                break;
            case KeyCoverage::Partition:
                any_represented = true;
                break;
            case KeyCoverage::None:
                rest.emplace_back(conjunct, reading.getContext());
                break;
        }
    }
    if (!any_represented)
        return {};

    Float64 share = 1.0;
    if (!primary_key_conjuncts.empty())
    {
        const auto measured = primaryKeyShare(reading, analysis.parts_with_ranges, primary_key_conjuncts);
        if (!measured)
            return {};
        share = *measured;
    }
    auto profile = estimator.estimateRelationProfile(metadata, rest, share);
    profile.estimate_from_defaults = true;
    return profile;
}

/// Sum of two row counts, unknown when either is; saturates instead of wrapping.
std::optional<UInt64> addRows(std::optional<UInt64> left, std::optional<UInt64> right)
{
    if (!left || !right)
        return {};
    return saturatingAdd(*left, *right);
}

/// A row count times a factor, unknown when the count is; saturates instead of wrapping.
std::optional<UInt64> multiplyRows(std::optional<UInt64> rows, UInt64 factor)
{
    if (!rows)
        return {};
    return saturatingMul(*rows, factor);
}

}

RelationProfile estimateReadProfile(
    const ReadFromMergeTree & reading,
    const ConditionSelectivityEstimator & estimator,
    const StorageMetadataPtr & metadata,
    const ReadFromMergeTree::AnalysisResult * analysis,
    const ActionsDAG::Node * filter,
    const ActionsDAG::Node * prewhere)
{
    auto profile = estimator.estimateRelationProfile(metadata, filter, prewhere);
    if (profile.estimate_from_defaults && analysis)
        if (auto refined = estimateWithKeyAnalysis(reading, estimator, metadata, *analysis, filter, prewhere))
            profile = std::move(*refined);
    return profile;
}

RelationStats estimateGroupStats(const Names & keys, const RelationStats & input_stats)
{
    RelationStats aggregation_stats;
    /// Carry imprecision, defaults and source from the input, or the annotation is lost for aggregation subqueries.
    aggregation_stats.imprecise_estimate = input_stats.imprecise_estimate;
    aggregation_stats.estimate_from_defaults = input_stats.estimate_from_defaults;
    aggregation_stats.source = input_stats.source;
    std::vector<GroupKey> group_keys;
    for (const auto & key : keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        UInt64 distinct_values = key_stats == input_stats.column_stats.end() ? 0 : key_stats->second.num_distinct_values;
        /// A key has at most as many values as the input has rows.
        if (distinct_values && input_stats.max_rows)
            distinct_values = std::min(distinct_values, *input_stats.max_rows);
        if (key_stats != input_stats.column_stats.end() && key_stats->second.hasImpreciseDistinctValues())
            aggregation_stats.imprecise_estimate = true;
        if (distinct_values == 0)
        {
            /// A key without an NDV leaves the group count to the other keys, or unknown, and marks
            /// the estimate as missing statistics.
            aggregation_stats.imprecise_estimate = true;
            if (aggregation_stats.source == RowEstimateSource::Statistics || aggregation_stats.source == RowEstimateSource::NoSource)
                aggregation_stats.source = RowEstimateSource::NoStatistics;
        }
        /// The key keeps its domain and width; its NDV is the input's.
        aggregation_stats.column_stats[key] = key_stats == input_stats.column_stats.end() ? ColumnStats{} : key_stats->second;
        auto & key_column_stats = aggregation_stats.column_stats[key];
        key_column_stats.num_distinct_values = distinct_values;
        group_keys.push_back({
            .name = key,
            .distinct_values = distinct_values,
            .table_rows = key_column_stats.table_rows,
            .table_selected_rows = key_column_stats.table_selected_rows});
    }

    if (keys.size() == 1)
        aggregation_stats.column_stats[keys.front()].unique = true;

    auto groups = estimateGroupCount(group_keys, input_stats.estimated_rows, input_stats.max_rows);
    /// Keys limited to constants by a filter below bound the groups by the product of their values.
    std::optional<UInt64> proven_groups = keys.empty() ? std::nullopt : std::optional<UInt64>(1);
    for (const auto & key : keys)
    {
        const UInt64 values = aggregation_stats.column_stats[key].max_distinct_values;
        proven_groups = values && proven_groups ? std::optional<UInt64>(saturatingMul(*proven_groups, values)) : std::nullopt;
    }
    if (proven_groups)
    {
        groups.max_rows = groups.max_rows ? std::min(*groups.max_rows, *proven_groups) : *proven_groups;
        if (groups.estimated_rows)
            groups.estimated_rows = std::min(*groups.estimated_rows, *proven_groups);
    }
    aggregation_stats.estimated_rows = groups.estimated_rows;
    aggregation_stats.max_rows = groups.max_rows;
    return aggregation_stats;
}

RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats)
{
    auto stats = estimateGroupStats(aggregating_step.getAggregatorParameters().keys, input_stats);
    /// `GROUPING SETS` emits the groups of every set: at most the groups of all the keys per set,
    /// and at most the input rows per set, which can exceed the input.
    if (aggregating_step.isGroupingSets())
    {
        const UInt64 sets = aggregating_step.getGroupingSetsParamsList().size();
        stats.estimated_rows = multiplyRows(stats.estimated_rows, sets);
        stats.max_rows = multiplyRows(input_stats.max_rows, sets);
        stats.imprecise_estimate = true;
        /// A key missing from a set is filled with a default or NULL: one more value, which a group of another set
        /// can have too, and which can be outside the value range.
        for (auto & [_, column_stats] : stats.column_stats)
        {
            if (column_stats.max_distinct_values)
                column_stats.max_distinct_values = saturatingAdd(column_stats.max_distinct_values, UInt64{1});
            column_stats.unique = false;
            column_stats.clearValueRange();
            column_stats.null_fraction.reset();
        }
    }
    return stats;
}

namespace
{
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

RelationStats estimateReadRowsCountImpl(QueryPlan::Node & node, const ActionsDAG::Node * filter, bool for_runtime_filter_transport);
}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, bool for_runtime_filter_transport)
{
    if (prunesNothingAtPlanTime(filter))
        filter = nullptr;

    auto stats = estimateReadRowsCountImpl(node, filter, for_runtime_filter_transport);
    /// A filter on the read proves the values it keeps whatever estimated the rows.
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(node.step.get()))
    {
        capDistinctValuesByFilter(filter, stats.column_stats);
        if (const auto & prewhere_info = reading->getPrewhereInfo())
            capDistinctValuesByFilter(
                prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name), stats.column_stats,
                &prewhere_info->prewhere_actions.getOutputs());
    }
    return stats;
}

namespace
{

/// Whether the read returns only values that the statistics of its parts describe.
bool readReturnsStoredValues(const ReadFromMergeTree & reading)
{
    const auto & data = reading.getMergeTreeData();
    /// A `FINAL` merge can make rows that are not stored (a sum, a merged aggregate state, a rolled up value, or a
    /// row made of the values of several rows); Replacing and the collapsing modes return some of the stored rows.
    if (reading.isQueryWithFinal())
    {
        using enum MergeTreeData::MergingParams::Mode;
        const auto mode = data.merging_params.mode;
        if (mode != Ordinary && mode != Replacing && mode != Collapsing && mode != VersionedCollapsing)
            return false;
    }
    /// The read applies the patches and mutations of its snapshot to the parts: updates, type conversions, and
    /// renames or drops that give a column name the values of another column or defaults.
    if (const auto snapshot = reading.getMutationsSnapshot();
        snapshot && (snapshot->hasPatchParts() || snapshot->hasDataMutations() || snapshot->hasAlterMutations() || snapshot->hasMetadataMutations()))
        return false;
    return !data.hasEnabledMaskingPolicies(reading.getContext());
}

RelationStats estimateReadRowsCountImpl(QueryPlan::Node & node, const ActionsDAG::Node * filter, bool for_runtime_filter_transport)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();

        /// Analyze partition and primary-key ranges before estimating the relation so column
        /// statistics come only from parts that can satisfy the query. Reuse the result for
        /// the index-based fallback below.
        const ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResultForEstimation();

        /// An exact empty range selection proves that the relation is empty. Other empty
        /// analysis results can be placeholders for deferred work, so only propagate zero
        /// when `has_exact_ranges` is set.
        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return RelationStats{.estimated_rows = 0, .max_rows = 0, .table_name = table_display_name};

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
        const bool has_filter = filter || has_prewhere_filter;

        /// Runtime-filter transport may raise its hard exact-phase row cap to this estimate. Under an
        /// unindexed plan-time filter, column statistics still produce a number, but it is only a guess
        /// and would override the exact limit from the settings. So the transport gets no estimate and
        /// keeps the settings geometry. Join-order estimation keeps using statistics.
        if (for_runtime_filter_transport && has_filter && !is_filtered_by_index)
        {
            return RelationStats{
                .estimated_rows = {},
                .max_rows = analyzed_result ? std::optional<UInt64>(analyzed_result->selected_rows) : std::nullopt,
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto relation_profile = estimateReadProfile(
                    *reading, *estimator, reading->getStorageMetadata(), analyzed_result.get(), filter, prewhere_node);
                const bool imprecise_rows = estimator->recordRowsWithoutStatistics(relation_profile);
                if (!readReturnsStoredValues(*reading))
                    for (auto & [_, column_stats] : relation_profile.column_stats)
                    {
                        column_stats.clearValueRange();
                        column_stats.null_fraction.reset();
                    }
                /// A selectivity estimate that rounds to zero rows does not prove the relation empty;
                /// one row keeps the joins above from taking it as empty.
                RelationStats stats{
                    .estimated_rows = std::max<UInt64>(relation_profile.rows, 1),
                    .max_rows = analyzed_result ? std::optional<UInt64>(analyzed_result->selected_rows) : std::nullopt,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .imprecise_estimate = imprecise_rows,
                    .estimate_from_defaults = relation_profile.estimate_from_defaults,
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
                .max_rows = reading->getStorageSnapshot()->storage.totalRows(reading->getContext()),
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};


        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{
                .estimated_rows = {},
                .max_rows = analyzed_result->selected_rows,
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};

        return RelationStats{
            .estimated_rows = analyzed_result->selected_rows,
            .max_rows = analyzed_result->selected_rows,
            .table_name = table_display_name,
            .imprecise_estimate = true,
            .source = RowEstimateSource::PrimaryIndex};
    }

    if (const auto * reading = typeid_cast<const ReadFromObjectStorageStep *>(step))
    {
        /// A data lake table knows its rows from its metadata; a plain object storage read does not.
        /// The count is the table's, so with a filter (pushed into the read or passed in) it is only
        /// a bound: there are no column statistics to estimate the filter by.
        RelationStats stats;
        stats.table_name = reading->getTableName();
        if (const auto total_rows = reading->totalRows())
        {
            stats.max_rows = total_rows;
            if (filter || reading->getFilterActionsDAG())
            {
                stats.imprecise_estimate = true;
                stats.source = RowEstimateSource::NoStatistics;
            }
            else
            {
                stats.estimated_rows = total_rows;
                stats.source = RowEstimateSource::Statistics;
            }
        }
        return stats;
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        std::optional<UInt64> total_rows = reading->getStorage()->totalRows({});
        String table_display_name = reading->getStorage()->getName();
        if (!total_rows)
            return RelationStats{.table_name = table_display_name, .imprecise_estimate = true, .source = RowEstimateSource::NoStatistics};
        return RelationStats{.estimated_rows = total_rows, .max_rows = total_rows, .table_name = table_display_name, .source = RowEstimateSource::Statistics};
    }

    /// We cannot do typeid_cast<const ReadFromSystemOneStep *>(step)
    /// since this is defined in clickhouse_storages_system module,
    /// which is not linked to current module
    if (step->getName() == "ReadFromSystemOne")
    {
        /// system.one always produces exactly one row — used to implement constant SELECTs like `SELECT 1`.
        return RelationStats{.estimated_rows = 1, .max_rows = 1, .table_name = "system.one"};
    }

    if (const auto * reading = typeid_cast<const CommonSubplanReferenceStep *>(step))
    {
        return estimateReadRowsCount(*reading->getSubplanReferenceRoot(), filter, for_runtime_filter_transport);
    }

    /// A buffered subquery result has the rows of the subplan that fills the buffer.
    if (const auto * reading = typeid_cast<const ReadFromCommonBufferStep *>(step))
    {
        return estimateReadRowsCount(*reading->getSubplanRoot(), filter, for_runtime_filter_transport);
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        return RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .max_rows = join_step->getResultRowsUpperBound(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = join_step->hasImpreciseEstimate(),
            .estimate_from_defaults = join_step->isEstimateFromDefaults()};
    }

    /// The plan of the chosen join order stands for the graph.
    if (const auto * graph_step = typeid_cast<const JoinGraphStep *>(step))
        return estimateReadRowsCount(*graph_step->getChosenOrderRoot(), filter, for_runtime_filter_transport);

    if (const auto * union_step = typeid_cast<const UnionStep *>(step))
    {
        /// `UNION ALL`: the rows and the bounds of the inputs add up, each unknown when an input's
        /// is; a column's values add up too, matched by position, unknown when an input's are. The
        /// row width is the inputs' widths weighted by their rows, known when every input's is.
        RelationStats stats;
        stats.estimated_rows = 0;
        stats.max_rows = 0;
        Float64 row_bytes = 0;
        bool row_bytes_known = true;
        const auto & input_headers = union_step->getInputHeaders();
        for (size_t child_index = 0; child_index < node.children.size(); ++child_index)
        {
            auto child_stats = estimateReadRowsCount(*node.children[child_index], filter, for_runtime_filter_transport);
            row_bytes_known &= child_stats.avg_row_bytes.has_value();
            row_bytes = unionWidth(
                row_bytes, Float64(stats.estimated_rows.value_or(0)), child_stats.avg_row_bytes.value_or(0), Float64(child_stats.estimated_rows.value_or(0)));
            if (child_index == 0)
                stats.column_stats = std::move(child_stats.column_stats);
            else if (child_index < input_headers.size())
                addUnionColumnStats(
                    stats.column_stats, input_headers[0]->getNames(), child_stats.column_stats, input_headers[child_index]->getNames(),
                    stats.estimated_rows.value_or(0), child_stats.estimated_rows.value_or(0));
            stats.estimated_rows = addRows(stats.estimated_rows, child_stats.estimated_rows);
            stats.max_rows = addRows(stats.max_rows, child_stats.max_rows);
            stats.imprecise_estimate |= child_stats.imprecise_estimate;
            stats.estimate_from_defaults |= child_stats.estimate_from_defaults;
        }
        if (row_bytes_known)
            stats.avg_row_bytes = row_bytes;
        return stats;
    }

    if (node.children.size() != 1)
        return {};

    if (const auto * distinct_step = typeid_cast<const DistinctStep *>(step))
    {
        /// A preliminary `DISTINCT` only reduces the rows the final one sees; the estimate is the
        /// final one's.
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        if (distinct_step->isPreliminary())
            return stats;
        return estimateGroupStats(distinct_step->getColumnNames(), stats);
    }

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        /// `WITH TIES` can keep every row equal to the last one, so the limit bounds nothing then. A
        /// missing estimate stays missing: the limit is a bound, not an estimate of the rows below it.
        std::optional<UInt64> kept_rows;
        if (!limit_step->withTies())
        {
            const auto limit = limit_step->getLimit();
            if (estimated.estimated_rows && *estimated.estimated_rows > limit)
                estimated.estimated_rows = limit;
            if (!estimated.max_rows || *estimated.max_rows > limit)
                estimated.max_rows = limit;
            kept_rows = estimated.max_rows;
        }
        limitColumnStats(estimated.column_stats, kept_rows);
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step);
        expression_step && !expression_step->getExpression().hasArrayJoin())
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto stats = estimateReadRowsCount(*node.children.front(), predicate, for_runtime_filter_transport);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        capDistinctValuesByFilter(predicate, stats.column_stats, &filter_step->getExpression().getOutputs());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        auto aggregation_stats = estimateAggregatingStepStats(*aggregating_step, stats);
        return aggregation_stats;
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        if (sorting_step->getLimit())
        {
            /// A missing estimate stays missing: the limit is a bound, not an estimate.
            if (stats.estimated_rows && *stats.estimated_rows > sorting_step->getLimit())
                stats.estimated_rows = sorting_step->getLimit();
            if (!stats.max_rows || *stats.max_rows > sorting_step->getLimit())
                stats.max_rows = sorting_step->getLimit();
            limitColumnStats(stats.column_stats, stats.max_rows);
        }
        return stats;
    }

    /// Estimates must see through exchanges: they do not change row counts, and an
    /// already-distributed subtree would otherwise report unknown cardinality, degrading
    /// broadcast-vs-shuffle and join order decisions.
    if (dynamic_cast<LogicalExchangeStep *>(step))
        return estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);

    return {};
}

}

}
}
