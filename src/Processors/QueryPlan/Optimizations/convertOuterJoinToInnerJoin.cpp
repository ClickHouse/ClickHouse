#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB::QueryPlanOptimizations
{

size_t tryConvertOuterJoinToInnerJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
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
    if (table_join.strictness() == JoinStrictness::Asof)
        return 0;

    /// TODO: Support join_use_nulls
    if (table_join.joinUseNulls())
        return 0;

    bool check_left_stream = false;
    bool check_right_stream = false;

    if (table_join.kind() == JoinKind::Left)
    {
        check_right_stream = true;
    }
    else if (table_join.kind() == JoinKind::Right)
    {
        check_left_stream = true;
    }
    else if (table_join.kind() == JoinKind::Full)
    {
        check_left_stream = true;
        check_right_stream = true;
    }
    else
    {
        return 0;
    }

    const auto & filter_dag = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    const auto & left_stream_input_header = join->getInputStreams().front().header;
    const auto & right_stream_input_header = join->getInputStreams().back().header;

    bool left_stream_safe = true;
    bool right_stream_safe = true;

    if (check_left_stream)
        left_stream_safe = filter_dag->isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, left_stream_input_header);

    if (check_right_stream)
        right_stream_safe = filter_dag->isFilterAlwaysFalseForDefaultValueInputs(filter_column_name, right_stream_input_header);

    if (!left_stream_safe || !right_stream_safe)
        return 0;

    auto updated_table_join = std::make_shared<TableJoin>(table_join);
    updated_table_join->setKind(JoinKind::Inner);

    auto updated_join = join->getJoin()->clone(updated_table_join, left_stream_input_header, right_stream_input_header);
    join->setJoin(std::move(updated_join));

    return 1;
}

}
