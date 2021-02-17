#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Common/typeid_cast.h>
#include <Columns/IColumn.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

static size_t tryAddNewFilterStep(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    const Names & allowed_inputs)
{
    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = static_cast<FilterStep *>(parent.get());
    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    bool removes_filter = filter->removesFilterColumn();

    // std::cerr << "Filter: \n" << expression->dumpDAG() << std::endl;

    auto split_filter = expression->splitActionsForFilter(filter_column_name, removes_filter, allowed_inputs);
    if (!split_filter)
        return 0;

    // std::cerr << "===============\n" << expression->dumpDAG() << std::endl;
    // std::cerr << "---------------\n" << split_filter->dumpDAG() << std::endl;

    const auto & index = expression->getIndex();
    auto it = index.begin();
    for (; it != index.end(); ++it)
        if ((*it)->result_name == filter_column_name)
            break;

    if (it == expression->getIndex().end())
    {
        if (!removes_filter)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                            filter_column_name, expression->dumpDAG());

        std::cerr << "replacing to expr because filter " << filter_column_name << " was removed\n";
        parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
    }
    else if ((*it)->column && isColumnConst(*(*it)->column))
    {
        std::cerr << "replacing to expr because filter is const\n";
        parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
    }

    /// Add new Filter step before Aggregating.
    /// Expression/Filter -> Aggregating -> Something
    auto & node = nodes.emplace_back();
    node.children.swap(child_node->children);
    child_node->children.emplace_back(&node);
    /// Expression/Filter -> Aggregating -> Filter -> Something

    /// New filter column is added to the end.
    auto split_filter_column_name = (*split_filter->getIndex().rbegin())->result_name;
    node.step = std::make_unique<FilterStep>(
            node.children.at(0)->step->getOutputStream(),
            std::move(split_filter), std::move(split_filter_column_name), true);

    return 3;
}

size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());

    if (!filter)
        return 0;

    if (auto * aggregating = typeid_cast<AggregatingStep *>(child.get()))
    {
        const auto & params = aggregating->getParams();

        Names keys;
        keys.reserve(params.keys.size());
        for (auto pos : params.keys)
            keys.push_back(params.src_header.getByPosition(pos).name);

        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, keys))
            return updated_steps;
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(child.get()))
    {
        const auto & array_join_actions = array_join->arrayJoin();
        const auto & keys = array_join_actions->columns;
        const auto & array_join_header = array_join->getInputStreams().front().header;

        Names allowed_inputs;
        for (const auto & column : array_join_header)
            if (keys.count(column.name) == 0)
                allowed_inputs.push_back(column.name);

        // for (const auto & name : allowed_inputs)
        //     std::cerr << name << std::endl;

        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs))
            return updated_steps;
    }

    return 0;
}

}
