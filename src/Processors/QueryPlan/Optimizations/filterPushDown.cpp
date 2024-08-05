#include <Columns/IColumn.h>

#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeAggregateFunction.h>

#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
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
#include <fmt/format.h>

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

/// Assert that `node->children` has at least `child_num` elements
static void checkChildrenSize(QueryPlan::Node * node, size_t child_num)
{
    auto & child = node->step;
    if (child_num > child->getInputStreams().size() || child_num > node->children.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of children: expected at least {}, got {} children and {} streams",
                        child_num, child->getInputStreams().size(), node->children.size());
}

static bool identifiersIsAmongAllGroupingSets(const GroupingSetsParamsList & grouping_sets_params, const NameSet & identifiers_in_predicate)
{
    for (const auto & grouping_set : grouping_sets_params)
    {
        for (const auto & identifier : identifiers_in_predicate)
        {
            if (std::find(grouping_set.used_keys.begin(), grouping_set.used_keys.end(), identifier) == grouping_set.used_keys.end())
                return false;
        }
    }
    return true;
}

static NameSet findIdentifiersOfNode(const ActionsDAG::Node * node)
{
    NameSet res;

    /// We treat all INPUT as identifier
    if (node->type == ActionsDAG::ActionType::INPUT)
    {
        res.emplace(node->result_name);
        return res;
    }

    std::queue<const ActionsDAG::Node *> queue;
    queue.push(node);

    while (!queue.empty())
    {
        const auto * top = queue.front();
        for (const auto * child : top->children)
        {
            if (child->type == ActionsDAG::ActionType::INPUT)
            {
                res.emplace(child->result_name);
            }
            else
            {
                /// Only push non INPUT child into the queue
                queue.push(child);
            }
        }
        queue.pop();
    }
    return res;
}

static ActionsDAGPtr splitFilter(QueryPlan::Node * parent_node, const Names & allowed_inputs, size_t child_idx = 0)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    bool removes_filter = filter->removesFilterColumn();

    const auto & all_inputs = child->getInputStreams()[child_idx].header.getColumnsWithTypeAndName();

    auto split_filter = expression->cloneActionsForFilterPushDown(filter_column_name, removes_filter, allowed_inputs, all_inputs);
    return split_filter;
}

static size_t
tryAddNewFilterStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const ActionsDAGPtr & split_filter,
                    bool can_remove_filter = true, size_t child_idx = 0)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();

    const auto * filter_node = expression->tryFindInOutputs(filter_column_name);
    if (!filter_node && !filter->removesFilterColumn())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, expression->dumpDAG());

    /// Filter column was replaced to constant.
    const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);

    /// Add new Filter step before Aggregating.
    /// Expression/Filter -> Aggregating -> Something
    auto & node = nodes.emplace_back();
    node.children.emplace_back(&node);

    std::swap(node.children[0], child_node->children[child_idx]);
    /// Expression/Filter -> Aggregating -> Filter -> Something

    /// New filter column is the first one.
    String split_filter_column_name = split_filter->getOutputs().front()->result_name;

    node.step = std::make_unique<FilterStep>(
        node.children.at(0)->step->getOutputStream(), split_filter, std::move(split_filter_column_name), can_remove_filter);

    if (auto * transforming_step = dynamic_cast<ITransformingStep *>(child.get()))
    {
        transforming_step->updateInputStream(node.step->getOutputStream());
    }
    else
    {
        if (auto * join = typeid_cast<JoinStep *>(child.get()))
        {
            join->updateInputStream(node.step->getOutputStream(), child_idx);
        }
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

static size_t
tryAddNewFilterStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Names & allowed_inputs,
                    bool can_remove_filter = true, size_t child_idx = 0)
{
    if (auto split_filter = splitFilter(parent_node, allowed_inputs, child_idx))
        return tryAddNewFilterStep(parent_node, nodes, split_filter, can_remove_filter, child_idx);
    return 0;
}


/// Push down filter through specified type of step
template <typename Step>
static size_t simplePushDownOverStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, QueryPlanStepPtr & child)
{
    if (typeid_cast<Step *>(child.get()))
    {
        Names allowed_inputs = child->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs))
            return updated_steps;
    }
    return 0;
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
        /// If aggregating is GROUPING SETS, and not all the identifiers exist in all
        /// of the grouping sets, we could not push the filter down.
        if (aggregating->isGroupingSets())
        {

            const auto & actions = filter->getExpression();
            const auto & filter_node = actions->findInOutputs(filter->getFilterColumnName());

            auto identifiers_in_predicate = findIdentifiersOfNode(&filter_node);

            if (!identifiersIsAmongAllGroupingSets(aggregating->getGroupingSetsParamsList(), identifiers_in_predicate))
                return 0;
        }

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

    if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(child.get()))
    {
        /// CreatingSets does not change header.
        /// We can push down filter and update header.
        /// Filter - DelayedCreatingSets - Something
        child = std::make_unique<DelayedCreatingSetsStep>(filter->getOutputStream(), delayed->detachSets(), delayed->getContext());
        std::swap(parent, child);
        std::swap(parent_node->children, child_node->children);
        std::swap(parent_node->children.front(), child_node->children.front());
        /// DelayedCreatingSets - Filter - Something
        return 2;
    }

    if (auto * totals_having = typeid_cast<TotalsHavingStep *>(child.get()))
    {
        /// If totals step has HAVING expression, skip it for now.
        /// TODO:
        /// We can merge HAVING expression with current filter.
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

    if (auto updated_steps = simplePushDownOverStep<DistinctStep>(parent_node, nodes, child))
        return updated_steps;

    auto * join = typeid_cast<JoinStep *>(child.get());
    auto * filled_join = typeid_cast<FilledJoinStep *>(child.get());

    if (join || filled_join)
    {
        auto join_push_down = [&](JoinKind kind) -> size_t
        {
            const auto & table_join = join ? join->getJoin()->getTableJoin() : filled_join->getJoin()->getTableJoin();

            /// Only inner, cross and left(/right) join are supported. Other types may generate default values for left table keys.
            /// So, if we push down a condition like `key != 0`, not all rows may be filtered.
            if (table_join.kind() != JoinKind::Inner && table_join.kind() != JoinKind::Cross && table_join.kind() != kind)
                return 0;

            /// There is no ASOF Right join, so we're talking about pushing to the right side
            if (kind == JoinKind::Right && table_join.strictness() == JoinStrictness::Asof)
                return 0;

            bool is_left = kind == JoinKind::Left;
            const auto & input_header = is_left ? child->getInputStreams().front().header : child->getInputStreams().back().header;
            const auto & res_header = child->getOutputStream().header;
            Names allowed_keys;
            const auto & source_columns = input_header.getNames();
            for (const auto & name : source_columns)
            {
                /// Skip key if it is renamed.
                /// I don't know if it is possible. Just in case.
                if (!input_header.has(name) || !res_header.has(name))
                    continue;

                /// Skip if type is changed. Push down expression expect equal types.
                if (!input_header.getByName(name).type->equals(*res_header.getByName(name).type))
                    continue;

                allowed_keys.push_back(name);
            }

            /// For left JOIN, push down to the first child; for right - to the second one.
            const auto child_idx = is_left ? 0 : 1;
            ActionsDAGPtr split_filter = splitFilter(parent_node, allowed_keys, child_idx);
            if (!split_filter)
                return 0;
            /*
             * We should check the presence of a split filter column name in `source_columns` to avoid removing the required column.
             *
             * Example:
             * A filter expression is `a AND b = c`, but `b` and `c` belong to another side of the join and not in `allowed_keys`, so the final split filter is just `a`.
             * In this case `a` can be in `source_columns` but not `and(a, equals(b, c))`.
             *
             * New filter column is the first one.
             */
            const String & split_filter_column_name = split_filter->getOutputs().front()->result_name;
            bool can_remove_filter = source_columns.end() == std::find(source_columns.begin(), source_columns.end(), split_filter_column_name);
            const size_t updated_steps = tryAddNewFilterStep(parent_node, nodes, split_filter, can_remove_filter, child_idx);
            if (updated_steps > 0)
            {
                LOG_DEBUG(getLogger("QueryPlanOptimizations"), "Pushed down filter {} to the {} side of join", split_filter_column_name, kind);
            }
            return updated_steps;
        };

        if (size_t updated_steps = join_push_down(JoinKind::Left))
            return updated_steps;

        /// For full sorting merge join we push down both to the left and right tables, because left and right streams are not independent.
        if (join && join->allowPushDownToRight())
        {
            if (size_t updated_steps = join_push_down(JoinKind::Right))
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

    if (auto * sorting = typeid_cast<SortingStep *>(child.get()))
    {
        const auto & sort_description = sorting->getSortDescription();
        auto sort_description_it = std::find_if(sort_description.begin(), sort_description.end(), [&](auto & sort_column_description)
        {
            return sort_column_description.column_name == filter->getFilterColumnName();
        });
        bool can_remove_filter = sort_description_it == sort_description.end();

        Names allowed_inputs = child->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs, can_remove_filter))
            return updated_steps;
    }

    if (const auto * join_filter_set_step = typeid_cast<CreateSetAndFilterOnTheFlyStep *>(child.get()))
    {
        const auto & filter_column_name = assert_cast<const FilterStep *>(parent_node->step.get())->getFilterColumnName();
        bool can_remove_filter = !join_filter_set_step->isColumnPartOfSetKey(filter_column_name);

        Names allowed_inputs = child->getOutputStream().header.getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, nodes, allowed_inputs, can_remove_filter))
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
