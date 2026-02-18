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
#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    auto group = memo.getGroup(expression->group_id);

    /// Statistics should have been derived before calling estimateCost
    chassert(group->statistics.has_value());

    ExpressionCost total_cost;
    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_input_group = memo.getGroup(left_input.group_id);
        auto right_input_group = memo.getGroup(right_input.group_id);
        auto left_best_implementation = left_input_group->getBestImplementation(left_input.required_properties).expression;
        auto right_best_implementation = right_input_group->getBestImplementation(right_input.required_properties).expression;
        total_cost = estimateHashJoinCost(*join_step, *group->statistics, *left_input_group->statistics, *right_input_group->statistics);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        total_cost = estimateReadCost(*read_step, *group->statistics);
    }
    else if (typeid_cast<FilterStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost.subtree_cost = 0.1 * input_group->statistics->estimated_row_count;
    }
    else if (typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost.subtree_cost = 0.1 * input_group->statistics->estimated_row_count;
    }
    else if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression_plan_step))
    {
        auto input_group = memo.getGroup(expression->inputs[0].group_id);
        total_cost = estimateAggregationCost(*aggregating_step, *group->statistics, *input_group->statistics);
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

}
