#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <limits>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_statistics_optimize;
}

void StatisticsDerivation::deriveStatistics(GroupId group_id)
{
    auto group = memo.getGroup(group_id);

    /// Statistics already derived for this group
    if (group->statistics.has_value())
        return;

    /// Pick the first logical expression to derive statistics from
    /// (all logical expressions in a group represent the same logical result)
    if (group->logical_expressions.empty())
    {
        LOG_WARNING(log, "Group #{} has no logical expressions", group_id);
        group->statistics = ExpressionStatistics();
        return;
    }

    auto expression = group->logical_expressions.front();
    IQueryPlanStep * plan_step = expression->getQueryPlanStep();

    /// Ensure all input groups have statistics first (bottom-up derivation)
    for (const auto & input : expression->inputs)
    {
        auto input_group = memo.getGroup(input.group_id);
        if (!input_group->statistics.has_value())
            deriveStatistics(input.group_id);
    }

    /// Derive statistics based on the step type
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_input_group = memo.getGroup(left_input.group_id);
        auto right_input_group = memo.getGroup(right_input.group_id);
        group->statistics = deriveJoinStatistics(*join_step, *left_input_group->statistics, *right_input_group->statistics);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(plan_step))
    {
        group->statistics = deriveReadStatistics(*read_step);
    }
    else if (const auto * filter_step = typeid_cast<FilterStep *>(plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        group->statistics = deriveFilterStatistics(*filter_step, *input_group->statistics);
    }
    else if (const auto * expression_step = typeid_cast<ExpressionStep *>(plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        group->statistics = deriveExpressionStatistics(*expression_step, *input_group->statistics);
    }
    else if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        group->statistics = deriveAggregatingStatistics(*aggregating_step, *input_group->statistics);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        group->statistics = *input_group->statistics;
    }
    else
    {
        group->statistics = ExpressionStatistics();
    }

    LOG_TEST(log, "Derived statistics for group #{}:\n{}",
        group_id, group->statistics->dump());
}

ExpressionStatistics StatisticsDerivation::deriveJoinStatistics(
    const JoinStepLogical & join_step,
    const ExpressionStatistics & left_statistics,
    const ExpressionStatistics & right_statistics)
{
    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.max_row_count = left_statistics.max_row_count * right_statistics.max_row_count;

    statistics.column_statistics.insert(left_statistics.column_statistics.begin(), left_statistics.column_statistics.end());
    statistics.column_statistics.insert(right_statistics.column_statistics.begin(), right_statistics.column_statistics.end());

    Float64 join_selectivity = 1.0;
    for (const auto & predicate_expression : join_step.getJoinOperator().expression)
    {
        const auto & predicate = predicate_expression.asBinaryPredicate();
        auto left_column_actions = get<1>(predicate);
        auto right_column_actions = get<2>(predicate);

        if (get<0>(predicate) != JoinConditionOperator::Equals || !left_column_actions || !right_column_actions)
        {
            /// TODO: add support for non-equality operators
            LOG_TEST(log, "Skipping predicate '{}'", predicate_expression.dump());
            continue;
        }

        if (left_column_actions.fromRight() && right_column_actions.fromLeft())
            std::swap(left_column_actions, right_column_actions);
        const auto & left_column = left_column_actions.getColumnName();
        const auto & right_column = right_column_actions.getColumnName();

        auto left_column_statistics = left_statistics.column_statistics.find(left_column);
        auto right_column_statistics = right_statistics.column_statistics.find(right_column);

        UInt64 left_number_of_distinct_values = 1;
        UInt64 right_number_of_distinct_values = 1;
        UInt64 min_number_of_distinct_values = UInt64(std::min(left_statistics.estimated_row_count, right_statistics.estimated_row_count));
        if (left_column_statistics != left_statistics.column_statistics.end())
        {
            left_number_of_distinct_values = left_column_statistics->second.num_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, left_number_of_distinct_values);
        }
        if (right_column_statistics != right_statistics.column_statistics.end())
        {
            right_number_of_distinct_values = right_column_statistics->second.num_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, right_number_of_distinct_values);
        }

        /// Estimate JOIN equality predicate selectivity as 1 / max(NDV(A), NDV(B)) based on assumption that distinct values have equal probabilities
        UInt64 max_number_of_distinct_values = std::max(left_number_of_distinct_values, right_number_of_distinct_values);
        Float64 predicate_selectivity = 1.0 / Float64(max_number_of_distinct_values);

        /// NDV for join predicate columns can decrease if the other column has smaller NDV
        statistics.column_statistics[left_column].num_distinct_values = min_number_of_distinct_values;
        statistics.column_statistics[right_column].num_distinct_values = min_number_of_distinct_values;

        LOG_TEST(log, "Predicate '{} = {}' selectivity: 1 / {}",
            left_column, right_column, 1.0 / predicate_selectivity);

        /// Multiply selectivities of predicates assuming they are independent
        join_selectivity *= predicate_selectivity;
    }

    statistics.estimated_row_count = left_statistics.estimated_row_count * right_statistics.estimated_row_count * join_selectivity;

    for (auto & column_statistics : statistics.column_statistics)
        if (Float64(column_statistics.second.num_distinct_values) > statistics.estimated_row_count)
            column_statistics.second.num_distinct_values = UInt64(statistics.estimated_row_count);

    if (statistics.estimated_row_count < 0.01)
    {
        LOG_TEST(log, "Possibly incorrect estimation result: {}\nleft stats: {}\nright stats: {}\njoin_selectivity: {}",
            statistics.dump(), left_statistics.dump(), right_statistics.dump(), join_selectivity);
    }

    return statistics;
}

ExpressionStatistics StatisticsDerivation::deriveReadStatistics(const ReadFromMergeTree & read_step)
{
    ExpressionStatistics statistics;
    const auto & table_name = read_step.getStorageID().getTableName();

    statistics.min_row_count = 0;
    statistics.max_row_count = Float64(read_step.getStorageSnapshot()->storage.totalRows(nullptr).value_or(std::numeric_limits<UInt64>::max()));

    ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_step.selectRangesToRead();
    if (analyzed_result)
    {
        statistics.estimated_row_count = Float64(analyzed_result->selected_rows);
        statistics.max_row_count = Float64(analyzed_result->selected_rows);
    }
    else
        statistics.estimated_row_count = 1000000;

    if (read_step.getContext()->getSettingsRef()[Setting::allow_statistics_optimize])
    {
        /// TODO: Move this to IOptimizerStatistics implementation
        if (auto estimator = read_step.getConditionSelectivityEstimator(read_step.getAllColumnNames()))
        {
            auto prewhere_info = read_step.getPrewhereInfo();
            const ActionsDAG::Node * prewhere_node = prewhere_info
                ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                : nullptr;
            auto relation_profile = estimator->estimateRelationProfile(nullptr, nullptr, prewhere_node);

            statistics.estimated_row_count = Float64(relation_profile.rows);
            statistics.max_row_count = std::max<Float64>(statistics.max_row_count, statistics.estimated_row_count);
            for (const auto & [column_name, column_stats] : relation_profile.column_stats)
                statistics.column_statistics[column_name].num_distinct_values = column_stats.num_distinct_values;

            LOG_TEST(log, "Estimate statistics for table {}: {}", table_name, statistics.dump());
            return statistics;
        }
    }

    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto column_ndv = statistics_lookup.getNumberOfDistinctValues(table_name, column_name);
        if (column_ndv)
            statistics.column_statistics[column_name].num_distinct_values = column_ndv.value();
    }

    auto cardinality_hint = statistics_lookup.getCardinality(table_name);
    if (cardinality_hint)
        statistics.estimated_row_count = std::min<Float64>(statistics.estimated_row_count, Float64(*cardinality_hint));

    return statistics;
}

namespace QueryPlanOptimizations
{
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);
}

ExpressionStatistics StatisticsDerivation::deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, filter_step.getExpression());
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, expression_step.getExpression());
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    Float64 max_key_number_of_distinct_values = 1;
    Float64 max_total_number_of_distinct_values = 1;
    ExpressionStatistics aggregation_statistics;
    for (const auto & key : aggregator_params.keys)
    {
        /// If stats are present set NDV to 10% of number of rows
        Float64 key_number_of_distinct_values = 0.1 * input_statistics.estimated_row_count;

        auto key_stats = input_statistics.column_statistics.find(key);
        if (key_stats != input_statistics.column_statistics.end())
        {
            key_number_of_distinct_values = Float64(key_stats->second.num_distinct_values);
            key_number_of_distinct_values = std::min(key_number_of_distinct_values, input_statistics.max_row_count);
        }

        aggregation_statistics.column_statistics[key].num_distinct_values = UInt64(key_number_of_distinct_values);

        max_key_number_of_distinct_values = std::max(max_key_number_of_distinct_values, key_number_of_distinct_values);
        max_total_number_of_distinct_values *= key_number_of_distinct_values;
    }

    aggregation_statistics.min_row_count = 0;
    /// Estimate cardinality of aggregation as max NDV across individual aggregation keys
    aggregation_statistics.estimated_row_count = std::min(max_key_number_of_distinct_values, input_statistics.estimated_row_count);
    /// Maximum number of rows is the product of all aggregation key NDVs
    aggregation_statistics.max_row_count = std::min(max_total_number_of_distinct_values, input_statistics.max_row_count);
    return aggregation_statistics;
}

}
