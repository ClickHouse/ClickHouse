#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Common/typeid_cast.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

namespace DB::QueryPlanOptimizations
{

size_t tryConvertOuterJoinToInnerJoinLegacy(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & child = child_node->step;
    auto * join = typeid_cast<JoinStep *>(child.get());
    if (!join || !join->getJoin()->isCloneSupported())
        return 0;

    const auto & table_join = join->getJoin()->getTableJoin();

    /// Any JOIN issue https://github.com/ClickHouse/ClickHouse/issues/66447
    /// Anti JOIN issue https://github.com/ClickHouse/ClickHouse/issues/67156
    if (table_join.strictness() != JoinStrictness::All)
        return 0;

    /// TODO: Support join_use_nulls
    if (table_join.joinUseNulls())
        return 0;

    bool check_left_stream = table_join.kind() == JoinKind::Right || table_join.kind() == JoinKind::Full;
    bool check_right_stream = table_join.kind() == JoinKind::Left || table_join.kind() == JoinKind::Full;

    if (!check_left_stream && !check_right_stream)
        return 0;

    const auto & filter_dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & left_stream_input_header = join->getInputHeaders().front();
    const auto & right_stream_input_header = join->getInputHeaders().back();

    bool left_stream_safe = true;
    bool right_stream_safe = true;

    if (check_left_stream)
        left_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *left_stream_input_header);

    if (check_right_stream)
        right_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *right_stream_input_header);

    if (!left_stream_safe || !right_stream_safe)
        return 0;

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->setKind(JoinKind::Inner);

    auto updated_join = join->getJoin()->clone(updated_table_join, left_stream_input_header, right_stream_input_header);
    join->setJoin(std::move(updated_join));

    return 1;
}

namespace
{

size_t tryConvertAnyOuterJoinToInnerJoin(
    FilterStep * filter,
    QueryPlan::Node * join_node,
    JoinStepLogical * join_step)
{
    auto & join_operator = join_step->getJoinOperator();

    bool check_left_stream = isRight(join_operator.kind);
    bool check_right_stream = isLeft(join_operator.kind);
    if (!check_left_stream && !check_right_stream)
        return 0;

    auto * interesting_side_plan_node = check_left_stream ? join_node->children.front() : join_node->children.back();
    const auto & interesting_side_header = interesting_side_plan_node->step->getOutputHeader();

    auto result_for_not_matched_rows = filterResultForNotMatchedRows(
        filter->getExpression(),
        filter->getFilterColumnName(),
        *interesting_side_header,
        /*allow_unknown_function_arguments=*/true);

    /// If not matched rows are not always filtered out, then we cannot convert ANY OUTER JOIN to INNER JOIN
    /// See discussion in https://github.com/ClickHouse/ClickHouse/issues/66447
    if (result_for_not_matched_rows != FilterResult::FALSE)
        return 0;

    auto key_dags = join_step->preCalculateKeys(join_node->children.front()->step->getOutputHeader(), join_node->children.back()->step->getOutputHeader());
    if (!key_dags)
        return 0;

    const auto & key_dags_interesting_side = check_left_stream ? key_dags->first : key_dags->second;
    NameSet join_keys_interesting_side = std::ranges::to<NameSet>(key_dags_interesting_side.keys | std::views::transform(
        [](const auto * e) { return e->result_name; }
    ));

    /// During scalar subquery decorrelation process, at the end there added renamings of useful columns.
    /// This is done to avoid name conflicts. Instead of JOIN condition like 'a = a' it becomes 'a = <subquery name>.a'.
    /// So, we need to revert such renaming to be able to compare columns with the aggregating step keys.
    if (auto * expr_step = typeid_cast<ExpressionStep *>(interesting_side_plan_node->step.get()))
    {
        for (const auto & output : expr_step->getExpression().getOutputs())
        {
            if (output->type == ActionsDAG::ActionType::ALIAS)
            {
                if (join_keys_interesting_side.contains(output->result_name))
                {
                    const auto * current_node = output->children.front();
                    /// Revert through possible chain of aliases.
                    while (current_node->type == ActionsDAG::ActionType::ALIAS)
                        current_node = current_node->children.front();

                    /// Found an alias for join key. Will revert it to original name.
                    join_keys_interesting_side.erase(output->result_name);
                    join_keys_interesting_side.insert(current_node->result_name);
                }
            }
        }
        interesting_side_plan_node = interesting_side_plan_node->children.front();
    }

    /// If there is no aggregating step on the "interesting" side, then we cannot guarantee that
    /// there are only unique rows.
    auto * aggregating_step = typeid_cast<AggregatingStep *>(interesting_side_plan_node->step.get());
    if (!aggregating_step)
        return 0;

    /// GROUPING SETS does not guarantee unique rows.
    if (aggregating_step->isGroupingSets())
        return 0;

    /// If JOIN condition uses all aggregation keys, then there always only one row to match for each row from the other side.
    /// This means that we can safely convert ANY OUTER JOIN to INNER JOIN.
    for (const auto & aggregation_key : aggregating_step->getParams().keys)
    {
        if (!join_keys_interesting_side.contains(aggregation_key))
            return 0;
    }

    join_operator.kind = JoinKind::Inner;
    join_operator.strictness = JoinStrictness::All;

    return 1;
}

}

size_t tryConvertOuterJoinToInnerJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (size_t legacy_result = tryConvertOuterJoinToInnerJoinLegacy(parent_node, nodes); legacy_result > 0)
        return legacy_result;

    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & child = child_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(child.get());
    if (!join)
        return 0;
    if (!join->typeChangingSides().empty())
        return 0;

    auto & join_operator = join->getJoinOperator();
    if (join_operator.strictness == JoinStrictness::Any)
        return tryConvertAnyOuterJoinToInnerJoin(filter, child_node, join);
    if (join_operator.strictness != JoinStrictness::All)
        return 0;
    bool check_left_stream = isRightOrFull(join_operator.kind);
    bool check_right_stream = isLeftOrFull(join_operator.kind);
    if (!check_left_stream && !check_right_stream)
        return 0;

    const auto & filter_dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & left_stream_input_header = join->getInputHeaders().front();
    const auto & right_stream_input_header = join->getInputHeaders().back();

    bool left_stream_safe = true;
    bool right_stream_safe = true;

    if (check_left_stream)
        left_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *left_stream_input_header);

    if (check_right_stream)
        right_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, *right_stream_input_header);

    if (!left_stream_safe || !right_stream_safe)
    {
        if (join_operator.kind == JoinKind::Full)
        {
            if (left_stream_safe)
            {
                /// Rows with default values in the left stream are always filtered out.
                join_operator.kind = JoinKind::Left;
                return 1;
            }
            if (right_stream_safe)
            {
                /// Rows with default values in the right stream are always filtered out.
                join_operator.kind = JoinKind::Right;
                return 1;
            }
        }
        return 0;
    }

    join_operator.kind = JoinKind::Inner;
    return 1;
}

}
