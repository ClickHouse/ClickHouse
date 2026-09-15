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
#include <Processors/QueryPlan/Optimizations/debugHelpers.h>
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
    aggregation_stats.imprecise_estimate = input_stats.imprecise_estimate;
    aggregation_stats.source = input_stats.source;
    for (const auto & key : aggregator_params.keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        if (key_stats == input_stats.column_stats.end())
        {
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

}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter)
{
    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResult();
        if (!analyzed_result)
        {
            const auto & settings = reading->getContext()->getSettingsRef();
            const bool has_throwing_row_limit
                = (settings[Setting::read_overflow_mode] == OverflowMode::THROW && settings[Setting::max_rows_to_read])
                || (settings[Setting::read_overflow_mode_leaf] == OverflowMode::THROW && settings[Setting::max_rows_to_read_leaf]);
            analyzed_result = has_throwing_row_limit ? reading->selectRangesToReadForEstimation() : reading->selectRangesToRead();
        }

        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return RelationStats{.estimated_rows = 0, .table_name = table_display_name};

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

    if (step->getName() == "ReadFromSystemOne")
        return RelationStats{.estimated_rows = 1, .table_name = "system.one"};

    if (const auto * reading = typeid_cast<const CommonSubplanReferenceStep *>(step))
        return estimateReadRowsCount(*reading->getSubplanReferenceRoot(), filter);

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), filter);
        auto limit = limit_step->getLimit();
        if (!estimated.estimated_rows || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        return estimated;
    }

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step);
        expression_step && !expression_step->getExpression().hasArrayJoin())
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto stats = estimateReadRowsCount(*node.children.front(), predicate);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        return estimateAggregatingStepStats(*aggregating_step, stats);
    }

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        return RelationStats{
            .estimated_rows = join_step->getResultRowsEstimation(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = join_step->hasImpreciseEstimate()};
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter);
        if (sorting_step->getLimit() && (!stats.estimated_rows || stats.estimated_rows > sorting_step->getLimit()))
            stats.estimated_rows = sorting_step->getLimit();
        return stats;
    }

    if (dynamic_cast<LogicalExchangeStep *>(step))
        return estimateReadRowsCount(*node.children.front(), filter);

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return estimateReadRowsCount(*node.children.front(), filter);

    return {};
}

}
}
