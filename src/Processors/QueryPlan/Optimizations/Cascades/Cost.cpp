#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <base/types.h>

namespace DB
{

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        return estimateHashJoinCost(*join_step, expression->inputs[0], expression->inputs[1]);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        return estimateReadCost(*read_step);
    }
    else if (typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0]);
        return input_group->best_implementation.cost;
    }
    else
    {
        ExpressionCost total_cost;
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.subtree_cost = 100500;
            total_cost.number_of_rows = 100500;
        }
        /// Sum costs of the inputs
        for (auto input_group_id : expression->inputs)
        {
            const auto & input_group_cost = memo.getGroup(input_group_id)->best_implementation.cost;
            total_cost.subtree_cost += input_group_cost.subtree_cost;
            total_cost.number_of_rows += input_group_cost.number_of_rows;
        }
        return total_cost;
    }
}

ExpressionCost CostEstimator::estimateHashJoinCost(const JoinStepLogical & join_step, GroupId left_tree, GroupId right_tree)
{
    auto left_cost = memo.getGroup(left_tree)->best_implementation.cost;
    auto right_cost = memo.getGroup(right_tree)->best_implementation.cost;

//    if (join_step.areInputsSwapped())
//        std::swap(left_cost, right_cost);

    double join_selectivity = 1.0;
    for (const auto & predicate_expression : join_step.getJoinOperator().expression)
    {
        const auto & predicate = predicate_expression.asBinaryPredicate();
        const auto & left_column = get<1>(predicate).getColumnName();
        const auto & right_column = get<2>(predicate).getColumnName();
        auto left_number_of_distinct_values = statistics.getNumberOfDistinctValues(left_column);
        auto right_number_of_distinct_values = statistics.getNumberOfDistinctValues(right_column);

        UInt64 max_number_of_distinct_values = 1;
        if (left_number_of_distinct_values.has_value())
            max_number_of_distinct_values = left_number_of_distinct_values.value();
        if (right_number_of_distinct_values.has_value())
            max_number_of_distinct_values = std::max(max_number_of_distinct_values, right_number_of_distinct_values.value());

        /// Estimate JOIN equality predicate selectivity as 1 / max(NDV(A), NDV(B)) base on assumption that distinct values have equal probabilities
        Float64 predicate_selectivity = 1.0 / max_number_of_distinct_values;

        LOG_TEST(log, "Predicate '{} = {}' selectivity: 1 / max({}, {})",
            left_column, right_column, left_number_of_distinct_values.value_or(1), right_number_of_distinct_values.value_or(1));

        /// Multiply selectivities of predicates assuming they are independent
        join_selectivity *= predicate_selectivity;
    }
    ExpressionCost join_cost;
    join_cost.number_of_rows = Float64(left_cost.number_of_rows * right_cost.number_of_rows * join_selectivity);
    join_cost.subtree_cost =
        left_cost.subtree_cost + right_cost.subtree_cost + /// Cost of inputs
        left_cost.number_of_rows +      /// Scan of left table
        2 * right_cost.number_of_rows + /// Right table contributes more because we build hash table from it
        join_cost.number_of_rows;       /// Number of output rows

    return join_cost;
}

ExpressionCost CostEstimator::estimateReadCost(const ReadFromMergeTree & read_step)
{
    ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_step.selectRangesToRead();

    UInt64 selected_rows = 1000000;
    if (analyzed_result)
    {
        selected_rows = analyzed_result->selected_rows;
    }
    else if (auto total_rows = read_step.getStorageSnapshot()->storage.totalRows(nullptr); total_rows.has_value())
    {
        selected_rows = total_rows.value();
    }

    return ExpressionCost{
        .subtree_cost = Cost(selected_rows),
        .number_of_rows = Float64(selected_rows)
    };
}

}
