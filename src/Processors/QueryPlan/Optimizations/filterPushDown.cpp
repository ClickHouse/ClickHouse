#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <Columns/IColumn.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

static bool filterColumnIsNotAmongAggregatesArguments(const AggregateDescriptions & aggregates, const std::string & filter_column_name)
{
    for (const auto & aggregate : aggregates)
    {
        const auto & argument_names = aggregate.argument_names;
        if (std::find(argument_names.begin(), argument_names.end(), filter_column_name) != argument_names.end())
            return false;
    }
    return true;
}

static size_t
tryAddNewFilterStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Names & allowed_inputs, bool can_remove_filter = true)
{
    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = static_cast<FilterStep *>(parent.get());
    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    bool removes_filter = filter->removesFilterColumn();

    // std::cerr << "Filter: \n" << expression->dumpDAG() << std::endl;

    const auto & all_inputs = child->getInputStreams().front().header.getColumnsWithTypeAndName();

    auto split_filter = expression->cloneActionsForFilterPushDown(filter_column_name, removes_filter, allowed_inputs, all_inputs);
    if (!split_filter)
        return 0;

    // std::cerr << "===============\n" << expression->dumpDAG() << std::endl;
    // std::cerr << "---------------\n" << split_filter->dumpDAG() << std::endl;

    const auto * filter_node = expression->tryFindInIndex(filter_column_name);
    if (!filter_node && !removes_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, expression->dumpDAG());

    /// Filter column was replaced to constant.
    const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);

    /// Add new Filter step before Aggregating.
    /// Expression/Filter -> Aggregating -> Something
    auto & node = nodes.emplace_back();
    node.children.emplace_back(&node);
    std::swap(node.children[0], child_node->children[0]);
    /// Expression/Filter -> Aggregating -> Filter -> Something

    /// New filter column is the first one.
    auto split_filter_column_name = (*split_filter->getIndex().begin())->result_name;
    node.step = std::make_unique<FilterStep>(
        node.children.at(0)->step->getOutputStream(), std::move(split_filter), std::move(split_filter_column_name), can_remove_filter);

    if (auto * transforming_step = dynamic_cast<ITransformingStep *>(child.get()))
    {
        transforming_step->updateInputStream(node.step->getOutputStream());
    }
    else
    {
        if (auto * join = typeid_cast<JoinStep *>(child.get()))
            join->updateLeftStream(node.step->getOutputStream());
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "We are trying to push down a filter through a step for which we cannot update input stream");
    }

    if (!filter_node || filter_is_constant)
        /// This means that all predicates of filter were pushed down.
        /// Replace current actions to expression, as we don't need to filter anything.
        parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
    else
        filter->updateInputStream(child->getOutputStream());

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

    if (filter->getExpression()->hasStatefulFunctions())
        return 0;

    if (auto * aggregating = typeid_cast<AggregatingStep *>(child.get()))
    {
        const auto & params = aggregating->getParams();
        const auto & keys = params.keys;

        const bool filter_column_is_not_among_aggregation_keys
            = std::find(keys.begin(), keys.end(), filter->getFilterColumnName()) == keys.end();
        const bool can_remove_filter = filter_column_is_not_among_aggregation_keys
            && filterColumnIsNotAmongAggregatesArguments(params.aggregates, filter->getFilterColumnName());

        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, keys, can_remove_filter))
            return updated_steps;
    }

    if (typeid_cast<CreatingSetsStep *>(child.get()))
    {
        /// CreatingSets does not change header.
        /// We can push down filter and update header.
        ///                       - Something
        /// Filter - CreatingSets - CreatingSet
        ///                       - CreatingSet
        auto input_streams = child->getInputStreams();
        input_streams.front() = filter->getOutputStream();
        child = std::make_unique<CreatingSetsStep>(input_streams);
        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());
        ///              - Filter - Something
        /// CreatingSets - CreatingSet
        ///              - CreatingSet
        return 2;
    }

    if (auto * totals_having = typeid_cast<TotalsHavingStep *>(child.get()))
    {
        /// If totals step has HAVING expression, skip it for now.
        /// TODO:
        /// We can merge HAVING expression with current filer.
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
            if (!keys.contains(column.name))
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

    if (auto * join = typeid_cast<JoinStep *>(child.get()))
    {
        const auto & table_join  = join->getJoin()->getTableJoin();
        /// Push down is for left table only. We need to update JoinStep for push down into right.
        /// Only inner and left join are supported. Other types may generate default values for left table keys.
        /// So, if we push down a condition like `key != 0`, not all rows may be filtered.
        if (table_join.kind() == ASTTableJoin::Kind::Inner || table_join.kind() == ASTTableJoin::Kind::Left)
        {
            const auto & left_header = join->getInputStreams().front().header;
            const auto & res_header = join->getOutputStream().header;
            Names allowed_keys;
            const auto & source_columns = left_header.getNames();
            for (const auto & name : source_columns)
            {
                /// Skip key if it is renamed.
                /// I don't know if it is possible. Just in case.
                if (!left_header.has(name) || !res_header.has(name))
                    continue;

                /// Skip if type is changed. Push down expression expect equal types.
                if (!left_header.getByName(name).type->equals(*res_header.getByName(name).type))
                    continue;

                allowed_keys.push_back(name);
            }

            const bool can_remove_filter
                = std::find(source_columns.begin(), source_columns.end(), filter->getFilterColumnName()) == source_columns.end();
            if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_keys, can_remove_filter))
                return updated_steps;
        }
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

    if (typeid_cast<SortingStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto * union_step = typeid_cast<UnionStep *>(child.get()))
    {
        /// Union does not change header.
        /// We can push down filter and update header.
        auto union_input_streams = child->getInputStreams();
        for (auto & input_stream : union_input_streams)
            input_stream.header = filter->getOutputStream().header;

        ///                - Something
        /// Filter - Union - Something
        ///                - Something

        child = std::make_unique<UnionStep>(union_input_streams, union_step->getMaxThreads());

        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());

        ///       - Filter - Something
        /// Union - Something
        ///       - Something

        for (size_t i = 1; i < parent_node->children.size(); ++i)
        {
            auto & filter_node = nodes.emplace_back();
            filter_node.children.push_back(parent_node->children[i]);
            parent_node->children[i] = &filter_node;

            filter_node.step = std::make_unique<FilterStep>(
                filter_node.children.front()->step->getOutputStream(),
                filter->getExpression()->clone(),
                filter->getFilterColumnName(),
                filter->removesFilterColumn());
        }

        ///       - Filter - Something
        /// Union - Filter - Something
        ///       - Filter - Something

        return 3;
    }

    return 0;
}

}
