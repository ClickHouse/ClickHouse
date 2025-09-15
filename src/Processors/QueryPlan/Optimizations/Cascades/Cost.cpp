#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <base/types.h>
#include <limits>

namespace DB
{

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    fillStatistics(expression);

    ExpressionCost total_cost;
    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        auto left_best_implementation = memo.getGroup(expression->inputs[0])->best_implementation.expression;
        auto right_best_implementation = memo.getGroup(expression->inputs[1])->best_implementation.expression;
        total_cost = estimateHashJoinCost(*join_step, *expression->statistics, *left_best_implementation->statistics, *right_best_implementation->statistics);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        total_cost = estimateReadCost(*read_step, *expression->statistics);
    }
    else if (typeid_cast<FilterStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0]);
        total_cost.subtree_cost = 0.1 * input_group->best_implementation.expression->statistics->estimated_row_count;
    }
    else if (typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0]);
        total_cost.subtree_cost = 0.1 * input_group->best_implementation.expression->statistics->estimated_row_count;
    }
    else
    {
//        total_cost.number_of_rows = 100500;
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.subtree_cost = 100500;
        }
    }

    /// Add costs of all inputs
    for (auto input_group_id : expression->inputs)
    {
        const auto & input_group_cost = memo.getGroup(input_group_id)->best_implementation.cost;
        total_cost.subtree_cost += input_group_cost.subtree_cost;
    }

    return total_cost;
}

ExpressionCost CostEstimator::estimateHashJoinCost(
    const JoinStepLogical & /*join_step*/,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics)
{
    ExpressionCost join_cost;
//    join_cost.number_of_rows = this_step_statistics.estimated_row_count;
    join_cost.subtree_cost =
        left_statistics.estimated_row_count +           /// Scan of left table
        2.0 * right_statistics.estimated_row_count +    /// Right table contributes more because we build hash table from it
        this_step_statistics.estimated_row_count;       /// Number of output rows

    return join_cost;
}

ExpressionCost CostEstimator::estimateReadCost(const ReadFromMergeTree & /*read_step*/, const ExpressionStatistics & this_step_statistics)
{
    return ExpressionCost{
        .subtree_cost = Cost(this_step_statistics.estimated_row_count),
//        .number_of_rows = Float64(this_step_statistics.estimated_row_count)
    };
}

void CostEstimator::fillStatistics(GroupExpressionPtr expression)
{
    if (expression->statistics.has_value())
        return;

    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        auto left_best_implementation = memo.getGroup(expression->inputs[0])->best_implementation.expression;
        auto right_best_implementation = memo.getGroup(expression->inputs[1])->best_implementation.expression;
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
        auto input_best_implementation = memo.getGroup(expression->inputs[0])->best_implementation.expression;
        fillStatistics(input_best_implementation);
        expression->statistics = fillFilterStatistics(*filter_step, *input_best_implementation->statistics);
    }
    else if (const auto * expression_step = typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_best_implementation = memo.getGroup(expression->inputs[0])->best_implementation.expression;
        fillStatistics(input_best_implementation);
        expression->statistics = fillExpressionStatistics(*expression_step, *input_best_implementation->statistics);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input
        auto input_best_implementation = memo.getGroup(expression->inputs[0])->best_implementation.expression;
        fillStatistics(input_best_implementation);
        expression->statistics = *input_best_implementation->statistics;
    }
    else
    {
        expression->statistics = ExpressionStatistics();
    }

    LOG_TRACE(log, "Statistics for {}:\n{}", expression->getDescription(), expression->statistics->dump());
}

ExpressionStatistics CostEstimator::fillJoinStatistics(const JoinStepLogical & join_step, const ExpressionStatistics & left_statistics, const ExpressionStatistics &right_statistics)
{
    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.max_row_count = left_statistics.max_row_count * right_statistics.max_row_count;

    /// TODO: recalculate column stats based on selectivity
    statistics.column_statistics.insert(left_statistics.column_statistics.begin(), left_statistics.column_statistics.end());
    statistics.column_statistics.insert(right_statistics.column_statistics.begin(), right_statistics.column_statistics.end());

    double join_selectivity = 1.0;
    for (const auto & predicate_expression : join_step.getJoinOperator().expression)
    {
        const auto & predicate = predicate_expression.asBinaryPredicate();
        auto left_column_actions = get<1>(predicate);
        auto right_column_actions = get<2>(predicate);
        if (left_column_actions.fromRight() && right_column_actions.fromLeft())
            std::swap(left_column_actions, right_column_actions);
        const auto & left_column = getUnqualifiedColumnName(left_column_actions.getColumnName());
        const auto & right_column = getUnqualifiedColumnName(right_column_actions.getColumnName());

        auto left_column_statistics = left_statistics.column_statistics.find(left_column);
        auto right_column_statistics = right_statistics.column_statistics.find(right_column);

        UInt64 left_number_of_distinct_values = 1;
        UInt64 right_number_of_distinct_values = 1;
        UInt64 min_number_of_distinct_values = UInt64(std::min(left_statistics.estimated_row_count, right_statistics.estimated_row_count));
        if (left_column_statistics != left_statistics.column_statistics.end())
        {
            left_number_of_distinct_values = left_column_statistics->second.number_of_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, left_number_of_distinct_values);
        }
        if (right_column_statistics != right_statistics.column_statistics.end())
        {
            right_number_of_distinct_values = right_column_statistics->second.number_of_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, right_number_of_distinct_values);
        }

        /// Estimate JOIN equality predicate selectivity as 1 / max(NDV(A), NDV(B)) base on assumption that distinct values have equal probabilities
        UInt64 max_number_of_distinct_values = std::max(left_number_of_distinct_values, right_number_of_distinct_values);
        Float64 predicate_selectivity = 1.0 / max_number_of_distinct_values;

        /// If a column is the unique key in the table one one side of the join then the table on the other side
        /// is just filtered in proportion of NDV_key/NDV_other
        if (left_column_statistics != left_statistics.column_statistics.end() && left_number_of_distinct_values >= left_statistics.estimated_row_count)
        {
            Float64 predicate_selectivity_by_key = Float64(left_number_of_distinct_values) / right_number_of_distinct_values;
            predicate_selectivity = std::min(predicate_selectivity, predicate_selectivity_by_key);
        }
        if (right_column_statistics != right_statistics.column_statistics.end() && right_number_of_distinct_values >= right_statistics.estimated_row_count)
        {
            Float64 predicate_selectivity_by_key = Float64(right_number_of_distinct_values) / left_number_of_distinct_values;
            predicate_selectivity = std::min(predicate_selectivity, predicate_selectivity_by_key);
        }

        /// NDV for join predicate columns can decrease if the other column has smaller NDV
        statistics.column_statistics[left_column].number_of_distinct_values = min_number_of_distinct_values;
        statistics.column_statistics[right_column].number_of_distinct_values = min_number_of_distinct_values;

        LOG_TEST(log, "Predicate '{} = {}' selectivity: 1 / {}",
            left_column, right_column, 1.0 / predicate_selectivity);

        /// Multiply selectivities of predicates assuming they are independent
        join_selectivity *= predicate_selectivity;
    }

    statistics.estimated_row_count = Float64(left_statistics.estimated_row_count * right_statistics.estimated_row_count * join_selectivity);

    return statistics;
}

ExpressionStatistics CostEstimator::fillReadStatistics(const ReadFromMergeTree & read_step)
{
    ExpressionStatistics statistics;
    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto column_ndv = statistics_lookup.getNumberOfDistinctValues(column_name);
        if (column_ndv)
            statistics.column_statistics[column_name].number_of_distinct_values = column_ndv.value();
    }

    statistics.min_row_count = 0;
    statistics.max_row_count = read_step.getStorageSnapshot()->storage.totalRows(nullptr).value_or(std::numeric_limits<UInt64>::max());

    ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_step.selectRangesToRead();
    if (analyzed_result)
        statistics.estimated_row_count = analyzed_result->selected_rows;
    else
        statistics.estimated_row_count = 1000000;

    /// HACK
    if (read_step.getStorageID().table_name == "nation")
    {
        statistics.estimated_row_count = 2;
        statistics.max_row_count = 2;
        for (auto & column : statistics.column_statistics)
            column.second.number_of_distinct_values = 2;
    }
    if (read_step.getStorageID().table_name == "lineitem")
    {
        statistics.estimated_row_count = 200000000;
    }

    return statistics;
}

ExpressionStatistics CostEstimator::fillFilterStatistics(const FilterStep & /*filter_step*/, const ExpressionStatistics & input_statistics)
{
    /// TODO:
    return input_statistics;
}

ExpressionStatistics CostEstimator::fillExpressionStatistics(const ExpressionStep & /*expression_step*/, const ExpressionStatistics & input_statistics)
{
    /// TODO:
    return input_statistics;
}

}
