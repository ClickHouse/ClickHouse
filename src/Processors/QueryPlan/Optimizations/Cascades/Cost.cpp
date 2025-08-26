#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include "Processors/QueryPlan/JoinStep.h"

namespace DB
{

ExpressionCost CostEstimator::estimateCost(GroupExpressionPtr expression)
{
    if (const auto * join_step = typeid_cast<JoinStep *>(expression->plan_step.get()))
    {
        return estimateHashJoinCost(*join_step, expression->inputs[0], expression->inputs[1]);
    }
    return ExpressionCost{.subtree_cost = 2000000, .number_of_rows = 2000000};
}

ExpressionCost CostEstimator::estimateHashJoinCost(const JoinStep & join_step, GroupId left_tree, GroupId right_tree)
{
    auto left_cost = memo.getGroup(left_tree)->best_implementation.cost;
    auto right_cost = memo.getGroup(right_tree)->best_implementation.cost;

    (void)join_step;
    double join_selectivity = 0.01;

    ExpressionCost join_cost;
    join_cost.number_of_rows = UInt64(left_cost.number_of_rows * right_cost.number_of_rows * join_selectivity);
    join_cost.subtree_cost =
        left_cost.subtree_cost + right_cost.subtree_cost + /// Cost of inputs
        left_cost.number_of_rows +      /// Scan of left table
        2 * right_cost.number_of_rows + /// Right table contributes more because we build hash table from it 
        join_cost.number_of_rows;       /// Number of output rows

    return join_cost;
}

}
