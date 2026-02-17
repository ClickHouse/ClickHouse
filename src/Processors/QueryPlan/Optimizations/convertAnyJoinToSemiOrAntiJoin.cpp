#include <memory>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/IColumn.h>
#include <Common/logger_useful.h>
#include <Common/Logger.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Joins.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

FilterResult filterResultForMatchedRows(ActionsDAG pre_actions_dag, const ActionsDAG & filter_dag, const String & filter_column_name)
{
    auto combined_dag = ActionsDAG::merge(std::move(pre_actions_dag), filter_dag.clone());
    ActionsDAG::IntermediateExecutionResult combined_dag_input;

    ColumnsWithTypeAndName filter_output;
    try
    {
        filter_output = ActionsDAG::evaluatePartialResult(
            combined_dag_input,
            { combined_dag.tryFindInOutputs(filter_column_name) },
            /*input_rows_count=*/1,
            { .skip_materialize = true });
    }
    catch (...)
    {
        /// If we cannot evaluate the filter expression, return UNKNOWN
        return FilterResult::UNKNOWN;
    }

    return getFilterResult(filter_output[0]);
}

/// Check if filter has a form like "column" or "NOT column", where column is an input
bool isSimpleFilter(const ActionsDAG::Node * filter_node, bool must_negate)
{
    /// Skip aliases
    while (filter_node->type == ActionsDAG::ActionType::ALIAS)
        filter_node = filter_node->children.front();

    switch (filter_node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (must_negate && filter_node->function_base && filter_node->function_base->getName() == "not")
                return isSimpleFilter(filter_node->children.front(), false);
            return false;
        }
        case ActionsDAG::ActionType::INPUT:
            return !must_negate;
        default:
            return false;
    }
}

QueryPlanStepPtr convertToExpressionStep(FilterStep * filter_node)
{
    auto dag = std::move(filter_node->getExpression());
    if (filter_node->removesFilterColumn())
        dag.removeFromOutputs(filter_node->getFilterColumnName());
    auto new_expression_step = std::make_unique<ExpressionStep>(filter_node->getInputHeaders().front(), std::move(dag));
    new_expression_step->setStepDescription(*filter_node);
    return new_expression_step;
}

enum class JoinSide
{
    Left,
    Right
};

template <JoinSide join_side_to_check>
ActionsDAG getPreFilterActionsDAG(QueryPlan::Node * join_node, JoinStepLogical * join)
{
    if (auto * expression_before_join_step = typeid_cast<ExpressionStep *>(join_node->children[join_side_to_check == JoinSide::Left ? 0 : 1]->step.get()))
    {
        ActionsDAG result_dag;
        if constexpr (join_side_to_check == JoinSide::Right)
        {
            result_dag = ActionsDAG(join->getInputHeaders().front()->getColumnsWithTypeAndName());
            result_dag.unite(expression_before_join_step->getExpression().clone());
        }
        else
        {
            result_dag = expression_before_join_step->getExpression().clone();
            result_dag.unite(ActionsDAG(join->getInputHeaders().back()->getColumnsWithTypeAndName()));
        }
        result_dag.mergeInplace(join->getActionsDAG().clone());
        return result_dag;
    }

    return join->getActionsDAG().clone();
};

}

size_t tryConvertAnyJoinToSemiOrAntiJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & child = child_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(child.get());
    if (!join)
        return 0;

    auto & join_operator = join->getJoinOperator();
    if (join_operator.strictness != JoinStrictness::Any)
        return 0;

    if (!isLeftOrRight(join_operator.kind))
        return 0;

    const auto & filter_dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & left_stream_input_header = join->getInputHeaders().front();
    const auto & right_stream_input_header = join->getInputHeaders().back();

    switch (join_operator.kind)
    {
        case JoinKind::Left:
        {
            auto result_for_not_matched_rows = filterResultForNotMatchedRows(filter_dag, filter_column_name, *right_stream_input_header);
            auto result_for_matched_rows = filterResultForMatchedRows(getPreFilterActionsDAG<JoinSide::Right>(child_node, join), filter_dag, filter_column_name);

            if (result_for_not_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to SEMI JOIN");
                join_operator.strictness = JoinStrictness::Semi;
                if (result_for_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), false))
                {
                    /// Remove filter after SEMI JOIN because it's a constant expression that always evaluates to TRUE for matched rows
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after SEMI JOIN because it's always TRUE for matched rows", filter_column_name);
                    parent_node->step = convertToExpressionStep(filter);
                    return 2;
                }
                return 1;
            }
            if (result_for_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to ANTI JOIN");
                join_operator.strictness = JoinStrictness::Anti;
                if (result_for_not_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), true))
                {
                    /// Remove filter after ANTI JOIN because it's a constant expression that always evaluates to TRUE for not matched rows
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after ANTI JOIN because it's always TRUE for not matched rows", filter_column_name);
                    parent_node->step = convertToExpressionStep(filter);
                    return 2;
                }
                return 1;
            }
            return 0;
        }
        case JoinKind::Right:
        {
            auto result_for_not_matched_rows = filterResultForNotMatchedRows(filter_dag, filter_column_name, *left_stream_input_header);
            auto result_for_matched_rows = filterResultForMatchedRows(getPreFilterActionsDAG<JoinSide::Left>(child_node, join), filter_dag, filter_column_name);

            if (result_for_not_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to SEMI JOIN");
                join_operator.strictness = JoinStrictness::Semi;
                if (result_for_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), false))
                {
                    /// Remove filter after SEMI JOIN because it's a constant expression that always evaluates to TRUE for matched rows
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after SEMI JOIN because it's always TRUE for matched rows", filter_column_name);
                    parent_node->step = convertToExpressionStep(filter);
                    return 2;
                }
                return 1;
            }
            if (result_for_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to ANTI JOIN");
                join_operator.strictness = JoinStrictness::Anti;
                if (result_for_not_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), true))
                {
                    /// Remove filter after ANTI JOIN because it's a constant expression that always evaluates to TRUE for not matched rows
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after ANTI JOIN because it's always TRUE for not matched rows", filter_column_name);
                    parent_node->step = convertToExpressionStep(filter);
                    return 2;
                }
                return 1;
            }
            return 0;
        }
        default:
            return 0;
    }
}

}
