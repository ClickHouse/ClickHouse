#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
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

    LOG_DEBUG(getLogger(__func__), "Will evaluate filter for header: {}\n{}", interesting_side_header->dumpNames(), filter->getExpression().dumpDAG());
    auto result_for_not_matched_rows = filterResultForNotMatchedRows(
        filter->getExpression(),
        filter->getFilterColumnName(),
        *interesting_side_header,
        /*allow_unknown_function_arguments=*/true);

    LOG_DEBUG(getLogger(__func__), "Result for not matched rows on interesting side: {}", toString(result_for_not_matched_rows));

    if (result_for_not_matched_rows != FilterResult::FALSE)
        return 0;

    auto key_dags = join_step->preCalculateKeys(join_node->children.front()->step->getOutputHeader(), join_node->children.back()->step->getOutputHeader());
    if (!key_dags)
        return 0;

    auto get_node_name = [](const auto * e) { return e->result_name; };
    const auto & key_dags_interesting_side = check_left_stream ? key_dags->first : key_dags->second;
    NameSet join_keys_interesting_side = std::ranges::to<NameSet>(key_dags_interesting_side.keys | std::views::transform(get_node_name));

    /// TODO: Check expression
    if (auto * expr_step = typeid_cast<ExpressionStep *>(interesting_side_plan_node->step.get()))
    {
        LOG_DEBUG(getLogger(__func__), "Skipping expression step on interesting side:\n{}", expr_step->getExpression().dumpDAG());
        for (const auto & output : expr_step->getExpression().getOutputs())
        {
            if (output->type == ActionsDAG::ActionType::ALIAS)
            {
                if (join_keys_interesting_side.contains(output->result_name))
                {
                    /// Found an alias for join key. Will revert it to original name.
                    const auto & alias_child = output->children.front();
                    join_keys_interesting_side.erase(output->result_name);
                    join_keys_interesting_side.insert(alias_child->result_name);
                    LOG_DEBUG(getLogger(__func__), "Found alias for join key: {} -> {}", output->result_name, alias_child->result_name);
                }
            }
        }
        interesting_side_plan_node = interesting_side_plan_node->children.front();
    }

    auto * aggregating_step = typeid_cast<AggregatingStep *>(interesting_side_plan_node->step.get());
    if (!aggregating_step)
        return 0;

    LOG_DEBUG(getLogger(__func__), "Analyzing interesting side: {}", toString(aggregating_step->getParams().keys));


    for (const auto & key : join_keys_interesting_side)
        LOG_DEBUG(getLogger(__func__), "Analyzing interesting side (JOIN KEYS): {}", key);

    for (const auto & aggregation_key : aggregating_step->getParams().keys)
    {
        if (!join_keys_interesting_side.contains(aggregation_key))
        {
            LOG_DEBUG(getLogger(__func__), "Did not found key: {}", aggregation_key);
            return 0;
        }
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
    auto & join_operator = join->getJoinOperator();
    if (join_operator.strictness == JoinStrictness::Any)
        return tryConvertAnyOuterJoinToInnerJoin(filter, child_node, join);
    if (join_operator.strictness != JoinStrictness::All)
        return 0;
    if (!join->typeChangingSides().empty())
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
