#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static GroupPtr getInputGroupWithStats(Memo & memo, const GroupExpressionPtr & expression, size_t input_index)
{
    auto input_group = memo.getGroup(expression->inputs[input_index].group_id);
    if (!input_group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for input group #{} of expression '{}' (group #{}).\n"
            "Input group state:\n{}",
            expression->inputs[input_index].group_id, expression->getDescription(), expression->group_id, input_group->dump());
    return input_group;
}

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    auto group = memo.getGroup(expression->group_id);

    /// Statistics should have been derived before calling estimateCost
    if (!group->statistics.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CostEstimator: statistics not derived for group #{} (expression '{}') before estimateCost.\n"
            "Group state:\n{}",
            expression->group_id, expression->getDescription(), group->dump());

    const Float64 distribution_node_count = static_cast<Float64>(std::max<size_t>(expression->properties.distribution.node_count, 1));

    ExpressionCost total_cost;
    IQueryPlanStep * expression_plan_step = expression->getQueryPlanStep();
    if (const auto * join_step = typeid_cast<JoinStepLogical *>(expression_plan_step))
    {
        const auto & left_input = expression->inputs[0];
        const auto & right_input = expression->inputs[1];
        auto left_input_group = memo.getGroup(left_input.group_id);
        auto right_input_group = memo.getGroup(right_input.group_id);
        const auto * join_strategy = dynamic_cast<const IJoinStrategy *>(expression->strategy.get());
        total_cost = estimateHashJoinCost(*join_step, join_strategy, *group->statistics, *left_input_group->statistics, *right_input_group->statistics, distribution_node_count);
    }
    else if (const auto * read_step = typeid_cast<ReadFromMergeTree *>(expression_plan_step))
    {
        const auto * read_strategy = dynamic_cast<const IReadStrategy *>(expression->strategy.get());
        total_cost = estimateReadCost(*read_step, read_strategy, *group->statistics, distribution_node_count);
    }
    else if (typeid_cast<FilterStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.cpu = 0.1 * input_group->statistics->estimated_row_count;
    }
    else if (typeid_cast<ExpressionStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        total_cost.cost.cpu = 0.1 * input_group->statistics->estimated_row_count;
    }
    else if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        const auto * aggregation_strategy = dynamic_cast<const IAggregationStrategy *>(expression->strategy.get());
        total_cost = estimateAggregationCost(*aggregating_step, aggregation_strategy, *group->statistics, *input_group->statistics, distribution_node_count);
    }
    else if (typeid_cast<MergingAggregatedStep *>(expression_plan_step))
    {
        auto input_group = getInputGroupWithStats(memo, expression, 0);
        /// Merging intermediate aggregate states: CPU proportional to input + output rows.
        total_cost.cost.cpu = group->statistics->estimated_row_count + input_group->statistics->estimated_row_count;
    }
    else if (auto * broadcast = dynamic_cast<BroadcastExchangeStep *>(expression_plan_step))
    {
        auto result_count = static_cast<Float64>(broadcast->getResultBucketCount());
        /// Broadcast replicates all rows to every destination node.
        total_cost.cost.network += group->statistics->estimated_row_count * result_count;
        /// Each destination materializes the full dataset in memory.
        total_cost.cost.memory += group->statistics->estimated_row_count * result_count;
    }
    else if (dynamic_cast<LogicalExchangeStep *>(expression_plan_step))
    {
        /// Gather, Shuffle, Scatter: each row is sent exactly once.
        total_cost.cost.network += group->statistics->estimated_row_count;
    }
    else if (typeid_cast<SortingStep *>(expression_plan_step))
    {
        /// Sorting: CPU proportional to rows.
        total_cost.cost.cpu += group->statistics->estimated_row_count;
    }
    else
    {
        if (expression->inputs.empty())
        {
            /// Some default non-zero cost
            total_cost.cost.cpu = 100500;
        }
    }

    /// Subtree cost starts with the own cost of this expression, then children are added
    total_cost.subtree_cost = total_cost.cost;

    /// Add costs of all inputs
    for (const auto & input : expression->inputs)
    {
        const auto & input_group_cost = memo.getGroup(input.group_id)->getBestImplementation(input.required_properties).cost;
        total_cost.subtree_cost += input_group_cost.subtree_cost;
    }

    return total_cost;
}

ExpressionCost CostEstimator::estimateHashJoinCost(
    const JoinStepLogical & /*join_step*/,
        const IJoinStrategy * strategy,
        const ExpressionStatistics & this_step_statistics,
        const ExpressionStatistics & left_statistics,
        const ExpressionStatistics & right_statistics,
        Float64 distribution_node_count)
{
    const bool is_broadcast = dynamic_cast<const BroadcastJoinStrategy *>(strategy) != nullptr;
    const bool is_shuffle = dynamic_cast<const ShuffleJoinStrategy *>(strategy) != nullptr;

    ExpressionCost join_cost;
    join_cost.cost.cpu = this_step_statistics.estimated_row_count;       /// Number of output rows

    if (is_broadcast)
    {
        /// Hash table is built from the full right table on every node.
        /// Network cost is already modeled by the BroadcastExchange expression.
        join_cost.cost.memory += right_statistics.estimated_row_count * distribution_node_count;
    }
    else if (is_shuffle)
    {
        /// Hash table is built from right_rows/N on each node, total = right_rows.
        /// Network cost is already modeled by ShuffleExchange expressions.
        join_cost.cost.memory += right_statistics.estimated_row_count;
    }
    else
    {
        join_cost.cost.cpu +=
            left_statistics.estimated_row_count +           /// Scan of left table
            2.0 * right_statistics.estimated_row_count;     /// Right table contributes more because we build hash table from it

        /// HACK: Simulate spilling to disk if right table is too big
        if (right_statistics.estimated_row_count > 1)
            join_cost.cost.memory += 30 * right_statistics.estimated_row_count;
    }

    return join_cost;
}

ExpressionCost CostEstimator::estimateReadCost(
    const ReadFromMergeTree & /*read_step*/,
    const IReadStrategy * strategy,
    const ExpressionStatistics & this_step_statistics,
    Float64 distribution_node_count)
{
    /// FIXME: hack to simulate that parallel read is faster
    if (dynamic_cast<const ParallelReadStrategy *>(strategy) != nullptr)
    {
        return ExpressionCost{
            .cost = Cost{.io = this_step_statistics.estimated_row_count / distribution_node_count},
            .subtree_cost = {},
        };
    }

    return ExpressionCost{
        .cost = Cost{.io = this_step_statistics.estimated_row_count},
        .subtree_cost = {},
    };
}

ExpressionCost CostEstimator::estimateAggregationCost(
    const AggregatingStep & /*aggregating_step*/,
    const IAggregationStrategy * strategy,
    const ExpressionStatistics & this_step_statistics,
    const ExpressionStatistics & input_statistics,
    Float64 distribution_node_count)
{
    const bool is_local = dynamic_cast<const LocalAggregationStrategy *>(strategy) != nullptr;
    const bool is_shuffle = dynamic_cast<const ShuffleAggregationStrategy *>(strategy) != nullptr;
    const bool is_partial = dynamic_cast<const PartialAggregationStrategy *>(strategy) != nullptr;

    ExpressionCost aggregation_cost;

    if (is_local)
    {
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count +
            input_statistics.estimated_row_count;
    }
    else if (is_shuffle)
    {
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count / distribution_node_count +
            input_statistics.estimated_row_count / distribution_node_count;
        aggregation_cost.cost.network += input_statistics.estimated_row_count;
    }
    else if (is_partial)
    {
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count / distribution_node_count +
            input_statistics.estimated_row_count / distribution_node_count;
    }
    else
    {
        /// Fallback: no recognized strategy (e.g. DefaultImplementation passthrough).
        /// Same cost model as Local.
        aggregation_cost.cost.cpu +=
            this_step_statistics.estimated_row_count +
            input_statistics.estimated_row_count;
    }

    return aggregation_cost;
}

}
