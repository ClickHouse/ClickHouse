#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/IColumn.h>
#include <Core/Joins.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

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

bool isAlwaysFalse(const ColumnWithTypeAndName & column)
{
    if (!column.column)
        return false;

    auto which_constant_type = WhichDataType(column.type);
    if (!which_constant_type.isUInt8() && !which_constant_type.isNothing())
        return false;

    Field value;
    column.column->get(0, value);

    return value.isNull() || value.safeGet<UInt8>() == 0;
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
            bool filters_right_defaults = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *right_stream_input_header);
            if (filters_right_defaults)
            {
                join_info.strictness = JoinStrictness::Semi;
                return 1;
            }
            else
            {
                ActionsDAG::IntermediateExecutionResult pre_actions_input;
                const auto & right_pre_join_dag = join->getExpressionActions().right_pre_join_actions;

                if (auto * right_plan_step = typeid_cast<ExpressionStep *>(child_node->children[1]->step.get()))
                {
                    ActionsDAG::IntermediateExecutionResult empty_input;
                    auto partial_result = ActionsDAG::evaluatePartialResult(empty_input, right_plan_step->getExpression().getOutputs(), 1, false, true);

                    pre_actions_input = makeActionsInput(*right_pre_join_dag, partial_result);
                }

                auto pre_actions_output = ActionsDAG::evaluatePartialResult(pre_actions_input, right_pre_join_dag->getOutputs(), 1, false, true);
                ActionsDAG::IntermediateExecutionResult filter_input = makeActionsInput(filter_dag, pre_actions_output);

                auto filter_output = ActionsDAG::evaluatePartialResult(filter_input, { filter_dag.tryFindInOutputs(filter_column_name) }, 1, false, true);

                if (isAlwaysFalse(filter_output[0]))
                {
                    join_info.strictness = JoinStrictness::Anti;
                    return 1;
                }
            }
            return 0;
        }
        case JoinKind::Right:
        {
            bool filters_left_defaults = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *left_stream_input_header);
            if (filters_left_defaults)
            {
                join_info.strictness = JoinStrictness::Semi;
                return 1;
            }
            else
            {
                ActionsDAG::IntermediateExecutionResult pre_actions_input;
                const auto & left_pre_join_dag = join->getExpressionActions().left_pre_join_actions;

                if (auto * left_plan_step = typeid_cast<ExpressionStep *>(child_node->children[0]->step.get()))
                {
                    ActionsDAG::IntermediateExecutionResult empty_input;
                    auto partial_result = ActionsDAG::evaluatePartialResult(empty_input, left_plan_step->getExpression().getOutputs(), 1, false, true);

                    pre_actions_input = makeActionsInput(*left_pre_join_dag, partial_result);
                }

                auto pre_actions_output = ActionsDAG::evaluatePartialResult(pre_actions_input, left_pre_join_dag->getOutputs(), 1, false, true);
                ActionsDAG::IntermediateExecutionResult filter_input = makeActionsInput(filter_dag, pre_actions_output);

                auto filter_output = ActionsDAG::evaluatePartialResult(filter_input, { filter_dag.tryFindInOutputs(filter_column_name) }, 1, false, true);

                if (isAlwaysFalse(filter_output[0]))
                {
                    join_info.strictness = JoinStrictness::Anti;
                    return 1;
                }
            }
            return 0;
        }
        default:
            return 0;
    }    
}

}
