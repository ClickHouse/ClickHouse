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

static ActionsDAGPtr splitFilter(QueryPlan::Node * parent_node, const Names & available_inputs, size_t child_idx = 0)
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


    auto split_filter = expression->cloneActionsForFilterPushDown(filter_column_name, removes_filter, available_inputs, all_inputs);
    return split_filter;
}

static size_t
addNewFilterStepOrThrow(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const ActionsDAGPtr & split_filter,
                    bool can_remove_filter = true, size_t child_idx = 0, bool update_parent_filter = true)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();

    const auto * filter_node = expression->tryFindInOutputs(filter_column_name);
    if (update_parent_filter && !filter_node && !filter->removesFilterColumn())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, expression->dumpDAG());

    /// Add new Filter step before Child.
    /// Expression/Filter -> Child -> Something
    auto & node = nodes.emplace_back();
    node.children.emplace_back(&node);

    std::swap(node.children[0], child_node->children[child_idx]);
    /// Expression/Filter -> Child -> Filter -> Something

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

    if (update_parent_filter)
    {
        /// Filter column was replaced to constant.
        const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);

        if (!filter_node || filter_is_constant)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
        }
        else
        {
            filter->updateInputStream(child->getOutputStream());
        }
    }

    return 3;
}

static size_t
tryAddNewFilterStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Names & allowed_inputs,
                    bool can_remove_filter = true, size_t child_idx = 0)
{
    if (auto split_filter = splitFilter(parent_node, allowed_inputs, child_idx))
        return addNewFilterStepOrThrow(parent_node, nodes, split_filter, can_remove_filter, child_idx);
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

static size_t tryPushDownOverJoinStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, QueryPlanStepPtr & child)
{
    auto & parent = parent_node->step;
    auto * filter = assert_cast<FilterStep *>(parent.get());

    auto * join = typeid_cast<JoinStep *>(child.get());
    auto * filled_join = typeid_cast<FilledJoinStep *>(child.get());

    if (!join && !filled_join)
        return 0;

    /** For equivalent JOIN with condition `ON lhs.x_1 = rhs.y_1 AND lhs.x_2 = rhs.y_2 ...`, we can build equivalent sets of columns and this
      * will allow to push conditions that only use columns from equivalent sets to both sides of JOIN, without considering JOIN type.
      *
      * For example: `FROM lhs INNER JOIN rhs ON lhs.id = rhs.id AND lhs.value = rhs.value`
      * In this example columns `id` and `value` from both tables are equivalent.
      *
      * During filter push down for different JOIN types filter push down logic is different:
      *
      * 1. For INNER JOIN we can push all valid conditions to both sides of JOIN.
      * 2. For LEFT/RIGHT JOIN we can push conditions that use columns from left/right table to left/right JOIN side, and we can push conditions
      * that use only columns from equivalent sets to right/left JOIN side.
      * 3. For FULL OUTER JOIN we can push conditions that use only columns from equivalent sets to both JOIN sides.
      *
      * Additional filter push down optimizations:
      * 1. TODO: Support building equivalent sets for more than 2 JOINS. It is possible, but will require more complex analysis step.
      * 2. TODO: Support building equivalent sets for JOINs with more than 1 clause.
      * 3. TODO: For LEFT/RIGHT join support optimization, we can assume that RIGHT/LEFT columns used in filter will be default/NULL constants and
      * check if filter will always be false, in those scenario we can transform LEFT/RIGHT JOIN into INNER JOIN and push conditions to both tables.
      */

    const auto & left_stream_input_header = child->getInputStreams().front().header;
    const auto & right_stream_input_header = child->getInputStreams().back().header;
    const auto & join_header = child->getOutputStream().header;
    const auto & table_join = join ? join->getJoin()->getTableJoin() : filled_join->getJoin()->getTableJoin();

    ActionsDAGPtr left_table_filter_dag = filter->getExpression();
    std::string left_table_filter_column_name = filter->getFilterColumnName();

    ActionsDAGPtr right_table_filter_dag = filter->getExpression();
    std::string right_table_filter_column_name = filter->getFilterColumnName();

    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_left_table_key_column_name_to_right_table_column;
    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_right_table_key_column_name_to_left_table_column;

    bool has_single_clause = table_join.getClauses().size() == 1;

    if (has_single_clause)
    {
        const auto & join_clause = table_join.getClauses()[0];
        size_t key_names_size = join_clause.key_names_left.size();

        for (size_t i = 0; i < key_names_size; ++i)
        {
            const auto & left_table_key_name = join_clause.key_names_left[i];
            const auto & right_table_key_name = join_clause.key_names_right[i];

            const auto & left_table_column = left_stream_input_header.getByName(left_table_key_name);
            const auto & right_table_column = right_stream_input_header.getByName(right_table_key_name);

            if (!left_table_column.type->equals(*right_table_column.type))
                continue;

            equivalent_left_table_key_column_name_to_right_table_column[left_table_key_name] = right_table_column;
            equivalent_right_table_key_column_name_to_left_table_column[right_table_key_name] = left_table_column;
        }

        const auto & filter_expression = filter->getExpression();
        const auto * filter_expression_node = &filter_expression->findInOutputs(filter->getFilterColumnName());

        left_table_filter_dag = ActionsDAG::buildFilterActionsDAG({filter_expression_node}, equivalent_right_table_key_column_name_to_left_table_column);
        left_table_filter_column_name = left_table_filter_dag->getOutputs()[0]->result_name;

        right_table_filter_dag = ActionsDAG::buildFilterActionsDAG({filter_expression_node}, equivalent_left_table_key_column_name_to_right_table_column);
        right_table_filter_column_name = right_table_filter_dag->getOutputs()[0]->result_name;
    }
    else
    {
        right_table_filter_dag = right_table_filter_dag->clone();
    }

    ActionsDAGPtr left_stream_pushed_split_filter;
    ActionsDAGPtr right_stream_pushed_split_filter;

    auto join_push_down = [&](bool push_to_left_stream, bool filter_push_down_all_input_columns_available) -> size_t
    {
        const auto push_child_idx = push_to_left_stream ? 0 : 1;
        const auto & input_header = push_to_left_stream ? left_stream_input_header : right_stream_input_header;
        const auto & input_filter_expression = push_to_left_stream ? left_table_filter_dag : right_table_filter_dag;
        const auto & input_filter_column_name = push_to_left_stream ? left_table_filter_column_name : right_table_filter_column_name;
        const auto & equivalent_columns_for_filter = push_to_left_stream ? equivalent_left_table_key_column_name_to_right_table_column
                                                                         : equivalent_right_table_key_column_name_to_left_table_column;
        auto & stream_pushed_split_filter = push_to_left_stream ? left_stream_pushed_split_filter : right_stream_pushed_split_filter;

        Names available_input_columns_for_filter;
        const auto & input_columns_names = input_header.getNames();

        for (const auto & name : input_columns_names)
        {
            /// Skip columns that does not have equivalent column in other stream
            if (!filter_push_down_all_input_columns_available && !equivalent_columns_for_filter.contains(name))
                continue;

            /// Skip key if it is renamed.
            /// I don't know if it is possible. Just in case.
            if (!input_header.has(name) || !join_header.has(name))
                continue;

            /// Skip if type is changed. Push down expression expect equal types.
            if (!input_header.getByName(name).type->equals(*join_header.getByName(name).type))
                continue;

            available_input_columns_for_filter.push_back(name);
        }

        if (available_input_columns_for_filter.empty())
            return 0;

        stream_pushed_split_filter = input_filter_expression->cloneActionsForFilterPushDown(input_filter_column_name,
            filter->removesFilterColumn(),
            available_input_columns_for_filter,
            input_header.getColumnsWithTypeAndName());
        if (!stream_pushed_split_filter)
            return 0;

        /*
         * We should check the presence of a split filter column name in `input_columns_names` to avoid removing the required column.
         *
         * Example:
         * A filter expression is `a = c AND b = c`, but `b` and `c` belong to another side of the join and not in `allowed_keys`, so the final split filter is just `a`.
         * In this case `a` can be in `input_columns_names` but not `and(a, equals(b, c))`.
         *
         * New filter column is the first one.
         */
        const auto & split_filter_column_name = stream_pushed_split_filter->getOutputs().front()->result_name;
        bool can_remove_filter = std::find(input_columns_names.begin(), input_columns_names.end(), split_filter_column_name) != input_columns_names.end();
        const size_t updated_steps = addNewFilterStepOrThrow(parent_node, nodes, stream_pushed_split_filter, can_remove_filter, push_child_idx, false /*update_parent_filter*/);
        assert(updated_steps > 0);

        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
            "Pushed down filter {} to the {} side of join",
            split_filter_column_name,
            (push_to_left_stream ? JoinKind::Left : JoinKind::Right));

        return updated_steps;
    };

    bool left_stream_filter_push_down_all_input_columns_available = true;
    bool right_stream_filter_push_down_all_input_columns_available = true;

    if (table_join.kind() == JoinKind::Left)
    {
        right_stream_filter_push_down_all_input_columns_available = false;
    }
    else if (table_join.kind() == JoinKind::Right)
    {
        left_stream_filter_push_down_all_input_columns_available = false;
    }
    else if (table_join.kind() == JoinKind::Full)
    {
        left_stream_filter_push_down_all_input_columns_available = false;
        right_stream_filter_push_down_all_input_columns_available = false;
    }

    auto old_filter_expression = filter->getExpression();
    ActionsDAGPtr new_filter_expression;

    size_t left_stream_push_down_updated_steps = join_push_down(true /*push_to_left_stream*/, left_stream_filter_push_down_all_input_columns_available);
    size_t right_stream_push_down_updated_steps = 0;

    /** We disable push down to right table in cases:
      * 1. Right side is already filled. Example: JOIN with Dictionary.
      * 2. ASOF Right join is not supported.
      */
    if (join && join->allowPushDownToRight() && table_join.strictness() != JoinStrictness::Asof)
        right_stream_push_down_updated_steps = join_push_down(false /*push_to_left_stream*/, right_stream_filter_push_down_all_input_columns_available);

    if (left_stream_push_down_updated_steps || right_stream_push_down_updated_steps)
    {
        new_filter_expression = std::move(left_table_filter_dag);

        if (table_join.kind() == JoinKind::Right)
            new_filter_expression = std::move(right_table_filter_dag);
    }

    if (new_filter_expression)
    {
        const auto * filter_node = new_filter_expression->tryFindInOutputs(filter->getFilterColumnName());
        if (!filter_node && !filter->removesFilterColumn())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                            filter->getFilterColumnName(), new_filter_expression->dumpDAG());

        /// Filter column was replaced to constant.
        const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);
        if (!filter_node || filter_is_constant)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputStream(), new_filter_expression);
        }
    }

    return left_stream_push_down_updated_steps;
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

    if (auto updated_steps = tryPushDownOverJoinStep(parent_node, nodes, child))
        return updated_steps;

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
