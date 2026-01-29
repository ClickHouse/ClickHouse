#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <base/types.h>
#include <limits>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_statistics_optimize;
}

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    fillStatistics(expression);

    ExpressionCost total_cost;
    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_best_implementation = memo.getGroup(left_input.group_id)->getBestImplementation(left_input.required_properties).expression;
        auto right_best_implementation = memo.getGroup(right_input.group_id)->getBestImplementation(right_input.required_properties).expression;
        total_cost = estimateHashJoinCost(*join_step, *expression->statistics, *left_best_implementation->statistics, *right_best_implementation->statistics);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        total_cost = estimateReadCost(*read_step, *expression->statistics);
    }
    else if (typeid_cast<FilterStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost.subtree_cost = 0.1 * input_group->getBestImplementation(expression->inputs[0].required_properties).expression->statistics->estimated_row_count;
    }
    else if (typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost.subtree_cost = 0.1 * input_group->getBestImplementation(expression->inputs[0].required_properties).expression->statistics->estimated_row_count;
    }
    else if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost = estimateAggregationCost(*aggregating_step, *expression->statistics, *input_group->getBestImplementation(expression->inputs[0].required_properties).expression->statistics);
    }
    else
    {
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.subtree_cost = 100500;
        }
    }

    /// Add costs of all inputs
    for (const auto & input : expression->inputs)
    {
        const auto & input_group_cost = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties).cost;
        total_cost.subtree_cost += input_group_cost.subtree_cost;
    }

    return total_cost;
}

ExpressionCost CostEstimator::estimateHashJoinCost(
    const JoinStepLogical & join_step,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics)
{
    /// TODO: better way to distinguish between implementations: maybe different step types?
    const bool is_broadcast = join_step.getStepDescription().contains("Broadcast");
    const bool is_shuffle = join_step.getStepDescription().contains("Shuffle");

    ExpressionCost join_cost;
    join_cost.subtree_cost = this_step_statistics.estimated_row_count;       /// Number of output rows

    const size_t node_count = 4;

    if (is_broadcast)
    {
        /// Add the cost of sending right table
        join_cost.subtree_cost += right_statistics.estimated_row_count * node_count;
        /// Add the cost of memory consumed by right table
        join_cost.subtree_cost += right_statistics.estimated_row_count * node_count;
    }
    else if (is_shuffle)
    {
        /// Add the cost of sending right table
        join_cost.subtree_cost += right_statistics.estimated_row_count;
        /// Add the cost of sending left table
        join_cost.subtree_cost += left_statistics.estimated_row_count;
    }
    else
    {
        join_cost.subtree_cost +=
            left_statistics.estimated_row_count +           /// Scan of left table
            2.0 * right_statistics.estimated_row_count;     /// Right table contributes more because we build hash table from it
    }

    return join_cost;
}

ExpressionCost CostEstimator::estimateReadCost(const ReadFromMergeTree & /*read_step*/, const ExpressionStatistics & this_step_statistics)
{
    return ExpressionCost{
        .subtree_cost = Cost(this_step_statistics.estimated_row_count),
    };
}

ExpressionCost CostEstimator::estimateAggregationCost(
    const AggregatingStep & aggregating_step,
    const ExpressionStatistics & this_step_statistics,
    const ExpressionStatistics & input_statistics)
{
    const bool is_local = aggregating_step.getStepDescription().contains("Local");
    const bool is_shuffle = aggregating_step.getStepDescription().contains("Shuffle");
    const bool is_partial = aggregating_step.getStepDescription().contains("Partial");

    ExpressionCost aggregation_cost;

    const size_t node_count = 4;

    if (is_local)
    {
        aggregation_cost.subtree_cost += this_step_statistics.estimated_row_count;
    }
    else if (is_shuffle)
    {
        aggregation_cost.subtree_cost +=
            this_step_statistics.estimated_row_count / node_count +
            input_statistics.estimated_row_count / node_count;
    }
    else if (is_partial)
    {
        aggregation_cost.subtree_cost += input_statistics.estimated_row_count;
    }

    return aggregation_cost;
}

void CostEstimator::fillStatistics(GroupExpressionPtr expression)
{
    if (expression->statistics.has_value())
        return;

    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_best_implementation = memo.getGroup(left_input.group_id)->getBestImplementation(left_input.required_properties).expression;
        auto right_best_implementation = memo.getGroup(right_input.group_id)->getBestImplementation(right_input.required_properties).expression;
        fillStatistics(left_best_implementation);
        fillStatistics(right_best_implementation);
        expression->statistics = fillJoinStatistics(*join_step, *left_best_implementation->statistics, *right_best_implementation->statistics);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        expression->statistics = fillReadStatistics(*read_step);
    }
    else if (const auto * filter_step = typeid_cast<FilterStep *>(expression_plan_step))
    {
        auto input_best_implementation = memo.getGroup(expression->inputs[0].group_id)->getBestImplementation(expression->inputs[0].required_properties).expression;
        fillStatistics(input_best_implementation);
        expression->statistics = fillFilterStatistics(*filter_step, *input_best_implementation->statistics);
    }
    else if (const auto * expression_step = typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_best_implementation = memo.getGroup(expression->inputs[0].group_id)->getBestImplementation(expression->inputs[0].required_properties).expression;
        fillStatistics(input_best_implementation);
        expression->statistics = fillExpressionStatistics(*expression_step, *input_best_implementation->statistics);
    }
    else if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression_plan_step))
    {
        auto input_best_implementation = memo.getGroup(expression->inputs[0].group_id)->getBestImplementation(expression->inputs[0].required_properties).expression;
        fillStatistics(input_best_implementation);
        expression->statistics = fillAggregatingStatistics(*aggregating_step, *input_best_implementation->statistics);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input
        auto input_best_implementation = memo.getGroup(expression->inputs[0].group_id)->getBestImplementation(expression->inputs[0].required_properties).expression;
        fillStatistics(input_best_implementation);
        expression->statistics = *input_best_implementation->statistics;
    }
    else
    {
        expression->statistics = ExpressionStatistics();
    }

    LOG_TEST(log, "Statistics for group #{} expression {}:\n{}",
        expression->group_id, expression->getDescription(), expression->statistics->dump());
}

ExpressionStatistics CostEstimator::fillJoinStatistics(const JoinStepLogical & join_step, const ExpressionStatistics & left_statistics, const ExpressionStatistics &right_statistics)
{
    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.max_row_count = left_statistics.max_row_count * right_statistics.max_row_count;

    /// TODO: recalculate column stats based on selectivity
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

    if (statistics.estimated_row_count < 0.01)
    {
        LOG_TEST(log, "Possibly incorrect estimation result: {}\nleft stats: {}\nright stats: {}\njoin_selectivity: {}",
            statistics.dump(), left_statistics.dump(), right_statistics.dump(), join_selectivity);
    }

    return statistics;
}

ExpressionStatistics CostEstimator::fillReadStatistics(const ReadFromMergeTree & read_step)
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
        if (auto estimator = read_step.getConditionSelectivityEstimator())
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

ExpressionStatistics CostEstimator::fillFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, filter_step.getExpression());
    return result_statistics;
}

ExpressionStatistics CostEstimator::fillExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, expression_step.getExpression());
    return result_statistics;
}

ExpressionStatistics CostEstimator::fillAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics)
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
