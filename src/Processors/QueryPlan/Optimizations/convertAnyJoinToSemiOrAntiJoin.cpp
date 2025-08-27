#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Interpreters/ActionsDAG.h"

namespace DB::QueryPlanOptimizations
{

namespace
{

ActionsDAG::IntermediateExecutionResult makeActionsInput(const ActionsDAG & dag, const ColumnsWithTypeAndName & input_columns)
{
    ActionsDAG::IntermediateExecutionResult input;

    std::unordered_map<std::string_view, ColumnWithTypeAndName> output_map;
    for (const auto & output : input_columns)
    {
        if (output.column)
            output_map[output.name] = output;
    }

    for (const auto * input_node : dag.getInputs())
    {
        auto it = output_map.find(input_node->result_name);
        if (it != output_map.end())
            input[input_node] = it->second;
    }

    return input;
}

ColumnsWithTypeAndName partialyEvaluateActionsChain(const std::vector<ActionsDAG *> & actions_chain)
{
    ActionsDAG::IntermediateExecutionResult current_input;
    for (size_t i = 0; i + 1 < actions_chain.size(); ++i)
    {
        auto partial_result = ActionsDAG::evaluatePartialResult(current_input, actions_chain[i]->getOutputs(), 1, false, true);

        current_input = makeActionsInput(*actions_chain[i + 1], partial_result);
    }

    return ActionsDAG::evaluatePartialResult(current_input, actions_chain.back()->getOutputs(), 1, false, true);
}

enum class FilterResult
{
    UNKNOWN,
    TRUE,
    FALSE,
};

FilterResult getFilterResult(const ColumnWithTypeAndName & column)
{
    if (!column.column)
        return FilterResult::UNKNOWN;

    auto which_constant_type = WhichDataType(column.type);
    if (!which_constant_type.isUInt8() && !which_constant_type.isNothing())
        return FilterResult::UNKNOWN;

    Field value;
    column.column->get(0, value);

    if (value.isNull())
        return FilterResult::FALSE;

    return value.safeGet<UInt8>() == 0 ? FilterResult::FALSE : FilterResult::TRUE;
}

FilterResult filterResultForMatchedRows(const std::vector<ActionsDAG *> & pre_actions_chain, const ActionsDAG & filter_dag, const String & filter_column_name)
{
    auto pre_actions_output = partialyEvaluateActionsChain(pre_actions_chain);

    ActionsDAG::IntermediateExecutionResult filter_input = makeActionsInput(filter_dag, pre_actions_output);
    auto filter_output = ActionsDAG::evaluatePartialResult(filter_input, { filter_dag.tryFindInOutputs(filter_column_name) }, 1, false, true);

    return getFilterResult(filter_output[0]);
}

FilterResult filterResultForNotMatchedRows(const ActionsDAG & filter_dag, const String & filter_column_name, const Block & input_stream_header)
{
    ActionsDAG::IntermediateExecutionResult filter_input;

    /// Create constant columns with default values for inputs of the filter DAG
    for (const auto * input : filter_dag.getInputs())
    {
        if (!input_stream_header.has(input->result_name))
            continue;

        if (input->column)
        {
            auto constant_column_with_type_and_name = ColumnWithTypeAndName{input->column, input->result_type, input->result_name};
            filter_input.emplace(input, std::move(constant_column_with_type_and_name));
            continue;
        }

        auto constant_column = input->result_type->createColumnConst(1, input->result_type->getDefault());
        auto constant_column_with_type_and_name = ColumnWithTypeAndName{constant_column, input->result_type, input->result_name};
        filter_input.emplace(input, std::move(constant_column_with_type_and_name));
    }

    auto filter_output = ActionsDAG::evaluatePartialResult(filter_input, { filter_dag.tryFindInOutputs(filter_column_name) }, 1, false, true);

    return getFilterResult(filter_output[0]);
}

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

    if (join->useNulls())
        return 0;

    auto & join_info = join->getJoinInfo();
    if (join_info.strictness != JoinStrictness::Any)
        return 0;

    if (!isLeftOrRight(join_info.kind))
        return 0;

    const auto & filter_dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & left_stream_input_header = join->getInputHeaders().front();
    const auto & right_stream_input_header = join->getInputHeaders().back();

    switch (join_info.kind)
    {
        case JoinKind::Left:
        {
            auto result_for_not_matched_rows = filterResultForNotMatchedRows(filter_dag, filter_column_name, *right_stream_input_header);

            std::vector<ActionsDAG *> actions_chain;
            if (auto * right_plan_step = typeid_cast<ExpressionStep *>(child_node->children[1]->step.get()))
                actions_chain.push_back(&right_plan_step->getExpression());
            actions_chain.push_back(join->getExpressionActions().right_pre_join_actions.get());

            auto result_for_matched_rows = filterResultForMatchedRows(actions_chain, filter_dag, filter_column_name);

            if (result_for_not_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to SEMI JOIN");
                join_info.strictness = JoinStrictness::Semi;
                if (result_for_matched_rows == FilterResult::TRUE)
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Can remove filter after SEMI JOIN");
                }
                else
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Cannot remove filter after SEMI JOIN: {}", result_for_not_matched_rows == FilterResult::FALSE ? "FALSE" : "UNKNOWN");
                }
                return 1;
            }
            if (result_for_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to ANTI JOIN");
                join_info.strictness = JoinStrictness::Anti;
                if (result_for_not_matched_rows == FilterResult::TRUE)
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Can remove filter after ANTI JOIN");
                }
                else
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Cannot remove filter after ANTI JOIN: {}", result_for_not_matched_rows == FilterResult::FALSE ? "FALSE" : "UNKNOWN");
                }
                return 1;
            }
            return 0;
        }
        case JoinKind::Right:
        {
            auto result_for_not_matched_rows = filterResultForNotMatchedRows(filter_dag, filter_column_name, *left_stream_input_header);

            std::vector<ActionsDAG *> actions_chain;
            if (auto * left_plan_step = typeid_cast<ExpressionStep *>(child_node->children[0]->step.get()))
                actions_chain.push_back(&left_plan_step->getExpression());
            actions_chain.push_back(join->getExpressionActions().left_pre_join_actions.get());

            auto result_for_matched_rows = filterResultForMatchedRows(actions_chain, filter_dag, filter_column_name);

            if (result_for_not_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to SEMI JOIN");
                join_info.strictness = JoinStrictness::Semi;
                if (result_for_matched_rows == FilterResult::TRUE)
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Can remove filter after SEMI JOIN");
                }
                else
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Cannot remove filter after SEMI JOIN: {}", result_for_not_matched_rows == FilterResult::FALSE ? "FALSE" : "UNKNOWN");
                }
                return 1;
            }
            if (result_for_matched_rows == FilterResult::FALSE)
            {
                LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to ANTI JOIN");
                join_info.strictness = JoinStrictness::Anti;
                if (result_for_not_matched_rows == FilterResult::TRUE)
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Can remove filter after ANTI JOIN");
                }
                else
                {
                    LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Cannot remove filter after ANTI JOIN: {}", result_for_not_matched_rows == FilterResult::FALSE ? "FALSE" : "UNKNOWN");
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
