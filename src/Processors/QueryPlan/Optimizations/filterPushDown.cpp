#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/FinishSortingStep.h>
#include <Processors/QueryPlan/MergeSortingStep.h>
#include <Processors/QueryPlan/MergingSortedStep.h>
#include <Processors/QueryPlan/PartialSortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>

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

        // std::cerr << "replacing to expr because filter " << filter_column_name << " was removed\n";
        parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
    }
    else if ((*it)->column && isColumnConst(*(*it)->column))
    {
        // std::cerr << "replacing to expr because filter is const\n";
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

static Names getAggregatinKeys(const Aggregator::Params & params)
{
    Names keys;
    keys.reserve(params.keys.size());
    for (auto pos : params.keys)
        keys.push_back(params.src_header.getByPosition(pos).name);

    return keys;
}

// static NameSet getColumnNamesFromSortDescription(const SortDescription & sort_desc, const Block & header)
// {
//     NameSet names;
//     for (const auto & column : sort_desc)
//     {
//         if (!column.column_name.empty())
//             names.insert(column.column_name);
//         else
//             names.insert(header.safeGetByPosition(column.column_number).name);
//     }

//     return names;
// }

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

    if (filter->getExpression()->hasStatefulFunctions())
        return 0;

    if (auto * aggregating = typeid_cast<AggregatingStep *>(child.get()))
    {
        const auto & params = aggregating->getParams();
        Names keys = getAggregatinKeys(params);

        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, keys))
            return updated_steps;
    }

    if (auto * totals_having = typeid_cast<TotalsHavingStep *>(child.get()))
    {
        /// If totals step has HAVING expression, skip it for now.
        /// TODO:
        /// We can merge HAING expression with current filer.
        /// Also, we can push down part of HAVING which depend only on aggregation keys.
        if (totals_having->getActions())
            return 0;

        Names keys;
        const auto & header = totals_having->getInputStreams().front().header;
        for (const auto & column : header)
            if (typeid_cast<const DataTypeAggregateFunction *>(column.type.get()) == nullptr)
                keys.push_back(column.name);

        /// NOTE: this optimization changes TOTALS value. Example:
        ///   `select * from (select y, sum(x) from (
        ///        select number as x, number % 4 as y from numbers(10)
        ///    ) group by y with totals) where y != 2`
        /// Optimization will replace totals row `y, sum(x)` from `(0, 45)` to `(0, 37)`.
        /// It is expected to ok, cause AST optimization `enable_optimize_predicate_expression = 1` also brakes it.
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

    if (auto * distinct = typeid_cast<DistinctStep *>(child.get()))
    {
        Names allowed_inputs = distinct->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs))
            return updated_steps;
    }

    /// TODO.
    /// We can filter earlier if expression does not depend on WITH FILL columns.
    /// But we cannot just push down condition, because other column may be filled with defaults.
    ///
    /// It is possible to filter columns before and after WITH FILL, but such change is not idempotent.
    /// So, appliying this to pair (Filter -> Filling) several times will create several similar filters.
    // if (auto * filling = typeid_cast<FillingStep *>(child.get()))
    // {
    // }

    /// Same reason for Cube
    // if (auto * cube = typeid_cast<CubeStep *>(child.get()))
    // {
    // }

    if (typeid_cast<PartialSortingStep *>(child.get())
        || typeid_cast<MergeSortingStep *>(child.get())
        || typeid_cast<MergingSortedStep *>(child.get())
        || typeid_cast<FinishSortingStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs))
            return updated_steps;
    }

    return 0;
}

}
