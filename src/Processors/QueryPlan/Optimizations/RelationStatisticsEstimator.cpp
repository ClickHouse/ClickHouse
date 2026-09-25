#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>
#include <ranges>
#include <vector>

#include <fmt/ranges.h>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
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
        return expression_step->getExpression().hasArrayJoin() ? UnaryStepStatsKind::Unsupported : UnaryStepStatsKind::Expression;
    }

    if (typeid_cast<const ArrayJoinStep *>(&step))
        return UnaryStepStatsKind::Unsupported;
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

std::optional<RelationStats> estimateLeafRelationStats(QueryPlan::Node & node, const ActionsDAG::Node * filter)
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

    return std::nullopt;
}

}

std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats)
{
    switch (classifyUnaryStepStats(step))
    {
        case UnaryStepStatsKind::Limit: {
            const auto & limit_step = static_cast<const LimitStep &>(step);
            const auto limit = limit_step.getLimit();
            if (!input_stats.estimated_rows || input_stats.estimated_rows > limit)
                input_stats.estimated_rows = limit;
            clearColumnValueRanges(input_stats.column_stats);
            return input_stats;
        }
        case UnaryStepStatsKind::Expression: {
            const auto & expression_step = static_cast<const ExpressionStep &>(step);
            remapColumnStats(input_stats.column_stats, expression_step.getExpression());
            return input_stats;
        }
        case UnaryStepStatsKind::Filter: {
            const auto & filter_step = static_cast<const FilterStep &>(step);
            remapColumnStats(input_stats.column_stats, filter_step.getExpression());
            return input_stats;
        }
        case UnaryStepStatsKind::Aggregating: return estimateAggregatingStepStats(static_cast<const AggregatingStep &>(step), input_stats);
        case UnaryStepStatsKind::Sorting: {
            const auto & sorting_step = static_cast<const SortingStep &>(step);
            if (sorting_step.getLimit())
            {
                if (!input_stats.estimated_rows || input_stats.estimated_rows > sorting_step.getLimit())
                    input_stats.estimated_rows = sorting_step.getLimit();
                clearColumnValueRanges(input_stats.column_stats);
            }
            return input_stats;
        }
        case UnaryStepStatsKind::LogicalExchange:
            /// Estimates must see through exchanges: they do not change row counts, and an
            /// already-distributed subtree would otherwise report unknown cardinality, degrading
            /// broadcast-vs-shuffle and join order decisions.
            return input_stats;
        case UnaryStepStatsKind::PreservingTransform: return input_stats;
        case UnaryStepStatsKind::Unsupported: return std::nullopt;
    }
    return std::nullopt;
}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter)
{
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
    RelationStats result;

    while (!stack.empty())
    {
        auto & frame = stack.back();
        if (frame.kind == FrameKind::Unvisited)
        {
            if (auto leaf_stats = estimateLeafRelationStats(*frame.node, frame.filter))
            {
                result = std::move(*leaf_stats);
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
                if (frame.node->children.size() != 1)
                {
                    result = {};
                    stack.pop_back();
                    continue;
                }

                /// Keep the optimized-join case after the unary-child check to preserve the previous
                /// traversal behavior exactly; logical joins with another arity remain unsupported.
                if (const auto * join_step = typeid_cast<const JoinStepLogical *>(frame.node->step.get());
                    join_step && join_step->isOptimized())
                {
                    /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
                    /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
                    result = RelationStats{
                        .estimated_rows = join_step->getResultRowsEstimation(),
                        .column_stats = join_step->getResultColumnStats(),
                        .table_name = join_step->getReadableRelationName(),
                        .imprecise_estimate = join_step->hasImpreciseEstimate()};
                    stack.pop_back();
                    continue;
                }

                /// Unsupported steps stop before their children are visited, preserving the old
                /// fail-closed boundary while avoiding recursion through an irrelevant subtree.
                if (classifyUnaryStepStats(*frame.node->step) == UnaryStepStatsKind::Unsupported)
                {
                    result = {};
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

            auto * child = frame.child;
            const auto * child_filter = frame.child_filter;
            stack.push_back({.node = child, .filter = child_filter});
            continue;
        }

        if (frame.kind == FrameKind::Unary)
            result = estimateUnaryStepStats(*frame.node->step, std::move(result)).value_or(RelationStats{});
        stack.pop_back();
    }

    return result;
}

}
}
