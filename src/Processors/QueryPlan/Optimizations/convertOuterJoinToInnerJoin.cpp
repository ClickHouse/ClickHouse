#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

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
        left_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, left_stream_input_header);

    if (check_right_stream)
        right_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, right_stream_input_header);

    if (!left_stream_safe || !right_stream_safe)
        return 0;

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->setKind(JoinKind::Inner);

    auto updated_join = join->getJoin()->clone(updated_table_join, left_stream_input_header, right_stream_input_header);
    join->setJoin(std::move(updated_join));

    return 1;
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
    if (join_operator.strictness != JoinStrictness::All)
        return 0;
    if (join->changesColumnsType())
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
        left_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, left_stream_input_header);

    if (check_right_stream)
        right_stream_safe = filter_dag.isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, right_stream_input_header);

    if (!left_stream_safe || !right_stream_safe)
        return 0;
    join_operator.kind = JoinKind::Inner;
    return 1;
}

}
