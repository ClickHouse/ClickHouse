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
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/CustomMetricLogViewStep.h>
#include <Storages/StorageMerge.h>

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

/// Assert that `node->children` has at least `child_num` elements
static void checkChildrenSize(QueryPlan::Node * node, size_t child_num)
{
    auto & child = node->step;
    if (child_num > child->getInputHeaders().size() || child_num > node->children.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of children: expected at least {}, got {} children and {} streams",
                        child_num, child->getInputHeaders().size(), node->children.size());
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

static std::optional<ActionsDAG::ActionsForFilterPushDown> splitFilter(QueryPlan::Node * parent_node, bool step_changes_the_number_of_rows, const Names & available_inputs, size_t child_idx = 0)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    bool removes_filter = filter->removesFilterColumn();

    const auto & all_inputs = child->getInputHeaders()[child_idx].getColumnsWithTypeAndName();
    bool allow_deterministic_functions = !step_changes_the_number_of_rows;
    return expression.splitActionsForFilterPushDown(filter_column_name, removes_filter, available_inputs, all_inputs, allow_deterministic_functions);
}

static size_t addNewFilterStepOrThrow(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    ActionsDAG::ActionsForFilterPushDown split_filter,
    size_t child_idx = 0, bool update_parent_filter = true)
{
    QueryPlan::Node * child_node = parent_node->children.front();
    checkChildrenSize(child_node, child_idx + 1);

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = assert_cast<FilterStep *>(parent.get());
    auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();

    const auto * filter_node = expression.tryFindInOutputs(filter_column_name);
    if (update_parent_filter && !filter_node && !filter->removesFilterColumn())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, expression.dumpDAG());

    /// Add new Filter step before Child.
    /// Expression/Filter -> Child -> Something
    auto & node = nodes.emplace_back();
    node.children.emplace_back(&node);

    std::swap(node.children[0], child_node->children[child_idx]);
    /// Expression/Filter -> Child -> Filter -> Something

    /// New filter column is the first one.
    String split_filter_column_name = split_filter.dag.getOutputs()[split_filter.filter_pos]->result_name;
    node.step = std::make_unique<FilterStep>(
        node.children.at(0)->step->getOutputHeader(), std::move(split_filter.dag), std::move(split_filter_column_name), split_filter.remove_filter);
    node.step->setStepDescription(filter->getStepDescription());

    child->updateInputHeader(node.step->getOutputHeader(), child_idx);

    if (update_parent_filter)
    {
        /// Filter column was replaced to constant.
        const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);

        if (!filter_node || filter_is_constant)
        {
            auto filter_description = filter->getStepDescription();

            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(expression));
            parent->setStepDescription(std::move(filter_description));
        }
        else
        {
            filter->updateInputHeader(child->getOutputHeader());
        }
    }

    return 3;
}

static size_t tryAddNewFilterStep(
    QueryPlan::Node * parent_node,
    bool step_changes_the_number_of_rows,
    QueryPlan::Nodes & nodes,
    const Names & allowed_inputs,
    size_t child_idx = 0)
{
    if (auto split_filter = splitFilter(parent_node, step_changes_the_number_of_rows, allowed_inputs, child_idx))
        return addNewFilterStepOrThrow(parent_node, nodes, std::move(*split_filter), child_idx);
    return 0;
}


/// Push down filter through specified type of step
template <typename Step>
static size_t simplePushDownOverStep(QueryPlan::Node * parent_node, bool step_changes_the_number_of_rows, QueryPlan::Nodes & nodes, QueryPlanStepPtr & child)
{
    if (typeid_cast<Step *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader().getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, step_changes_the_number_of_rows, nodes, allowed_inputs))
            return updated_steps;
    }
    return 0;
}

static void buildEquialentSetsForJoinStepLogical(
    std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_left_column,
    std::unordered_map<std::string, ColumnWithTypeAndName> & equivalent_right_column,
    const JoinInfo & join_info)
{
    if (!join_info.expression.disjunctive_conditions.empty())
        return;

    for (const auto & predicate : join_info.expression.condition.predicates)
    {
        auto left_column = predicate.left_node.getColumn();
        auto right_column = predicate.right_node.getColumn();

        if (predicate.op != PredicateOperator::Equals && predicate.op != PredicateOperator::NullSafeEquals)
            continue;
        if (!left_column.type->equals(*right_column.type))
            continue;
        equivalent_left_column[left_column.name] = right_column;
        equivalent_right_column[right_column.name] = left_column;
    }
}

static size_t tryPushDownOverJoinStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, QueryPlanStepPtr & child)
{
    auto & parent = parent_node->step;
    auto * filter = assert_cast<FilterStep *>(parent.get());

    auto * logical_join = typeid_cast<JoinStepLogical *>(child.get());
    auto * join = typeid_cast<JoinStep *>(child.get());
    auto * filled_join = typeid_cast<FilledJoinStep *>(child.get());

    if (!join && !filled_join && !logical_join)
        return 0;

    /** For equivalent JOIN with condition `ON lhs.x_1 = rhs.y_1 AND lhs.x_2 = rhs.y_2 ...`, we can build equivalent sets of columns and this
      * will allow to push conditions that only use columns from equivalent sets to both sides of JOIN, without considering JOIN type.
      *
      * For example: `FROM lhs INNER JOIN rhs ON lhs.id = rhs.id AND lhs.value = rhs.value`
      * In this example columns `id` and `value` from both tables are equivalent.
      *
      * During filter push down for different JOIN types filter push down logic is different:
      *
      * 1. For INNER JOIN we can push all valid conditions to both sides of JOIN. We also can push all valid conditions that use columns from
      * equivalent sets to both sides of JOIN.
      * 2. For LEFT/RIGHT JOIN we can push conditions that use columns from LEFT/RIGHT stream to LEFT/RIGHT JOIN side. We can also push conditions
      * that use columns from LEFT/RIGHT equivalent sets to RIGHT/LEFT JOIN side.
      *
      * Additional filter push down optimizations:
      * 1. TODO: Support building equivalent sets for more than 2 JOINS. It is possible, but will require more complex analysis step.
      * 2. TODO: Support building equivalent sets for JOINs with more than 1 clause.
      * 3. TODO: It is possible to pull up filter conditions from LEFT/RIGHT stream and push conditions that use columns from LEFT/RIGHT equivalent sets
      * to RIGHT/LEFT JOIN side.
      */

    const auto & join_header = child->getOutputHeader();
    const TableJoin * table_join_ptr = nullptr;
    if (join)
        table_join_ptr = &join->getJoin()->getTableJoin();
    else if (filled_join)
        table_join_ptr = &filled_join->getJoin()->getTableJoin();

    const auto & left_stream_input_header = child->getInputHeaders().front();
    const auto & right_stream_input_header = child->getInputHeaders().back();

    if (table_join_ptr && table_join_ptr->kind() == JoinKind::Full)
        return 0;
    if (logical_join && logical_join->getJoinInfo().kind == JoinKind::Full)
        return 0;

    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_left_stream_column_to_right_stream_column;
    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_right_stream_column_to_left_stream_column;

    bool has_single_clause = table_join_ptr && table_join_ptr->getClauses().size() == 1;
    if (has_single_clause && !filled_join)
    {
        const auto & join_clause = table_join_ptr->getClauses()[0];
        size_t key_names_size = join_clause.key_names_left.size();

        for (size_t i = 0; i < key_names_size; ++i)
        {
            const auto & left_table_key_name = join_clause.key_names_left[i];
            const auto & right_table_key_name = join_clause.key_names_right[i];
            const auto & left_table_column = left_stream_input_header.getByName(left_table_key_name);
            const auto & right_table_column = right_stream_input_header.getByName(right_table_key_name);

            if (!left_table_column.type->equals(*right_table_column.type))
                continue;

            equivalent_left_stream_column_to_right_stream_column[left_table_key_name] = right_table_column;
            equivalent_right_stream_column_to_left_stream_column[right_table_key_name] = left_table_column;
        }
    }
    else if (logical_join)
    {
        buildEquialentSetsForJoinStepLogical(
            equivalent_left_stream_column_to_right_stream_column,
            equivalent_right_stream_column_to_left_stream_column,
            logical_join->getJoinInfo());
    }

    auto get_available_columns_for_filter = [&](bool push_to_left_stream, bool filter_push_down_input_columns_available)
    {
        Names available_input_columns_for_filter;

        if (!filter_push_down_input_columns_available)
            return available_input_columns_for_filter;

        const auto & input_header = push_to_left_stream ? left_stream_input_header : right_stream_input_header;
        const auto & input_columns_names = input_header.getNames();

        for (const auto & name : input_columns_names)
        {
            if (!join_header.has(name))
                continue;

            /// Skip if type is changed. Push down expression expect equal types.
            if (!input_header.getByName(name).type->equals(*join_header.getByName(name).type))
                continue;

            available_input_columns_for_filter.push_back(name);
        }

        return available_input_columns_for_filter;
    };

    bool left_stream_filter_push_down_input_columns_available = true;
    bool right_stream_filter_push_down_input_columns_available = true;

    if (table_join_ptr && table_join_ptr->kind() == JoinKind::Left)
        right_stream_filter_push_down_input_columns_available = false;
    else if (table_join_ptr && table_join_ptr->kind() == JoinKind::Right)
        left_stream_filter_push_down_input_columns_available = false;

    if (logical_join && logical_join->getJoinInfo().kind == JoinKind::Left)
        right_stream_filter_push_down_input_columns_available = false;
    else if (logical_join && logical_join->getJoinInfo().kind == JoinKind::Right)
        left_stream_filter_push_down_input_columns_available = false;

    /** We disable push down to right table in cases:
      * 1. Right side is already filled. Example: JOIN with Dictionary.
      * 2. ASOF Right join is not supported.
      */
    bool allow_push_down_to_right = join && join->allowPushDownToRight() && table_join_ptr && table_join_ptr->strictness() != JoinStrictness::Asof;
    if (logical_join)
        allow_push_down_to_right = !logical_join->hasPreparedJoinStorage() && logical_join->getJoinInfo().strictness != JoinStrictness::Asof;

    if (!allow_push_down_to_right)
        right_stream_filter_push_down_input_columns_available = false;

    Names equivalent_columns_to_push_down;

    if (left_stream_filter_push_down_input_columns_available)
    {
        for (const auto & [name, _] : equivalent_left_stream_column_to_right_stream_column)
            equivalent_columns_to_push_down.push_back(name);
    }

    if (right_stream_filter_push_down_input_columns_available)
    {
        for (const auto & [name, _] : equivalent_right_stream_column_to_left_stream_column)
            equivalent_columns_to_push_down.push_back(name);
    }

    Names left_stream_available_columns_to_push_down = get_available_columns_for_filter(true /*push_to_left_stream*/, left_stream_filter_push_down_input_columns_available);
    Names right_stream_available_columns_to_push_down = get_available_columns_for_filter(false /*push_to_left_stream*/, right_stream_filter_push_down_input_columns_available);

    auto join_filter_push_down_actions = filter->getExpression().splitActionsForJOINFilterPushDown(filter->getFilterColumnName(),
        filter->removesFilterColumn(),
        left_stream_available_columns_to_push_down,
        left_stream_input_header,
        right_stream_available_columns_to_push_down,
        right_stream_input_header,
        equivalent_columns_to_push_down,
        equivalent_left_stream_column_to_right_stream_column,
        equivalent_right_stream_column_to_left_stream_column);

    size_t updated_steps = 0;

    /// For the logical join step, we need to merge pre-join actions to filter dag.
    /// TODO: this should be refactored and replaced with optimizations which
    /// 1. push filter/expression into JOIN (as post-filter)
    /// 2. move filter within JOIN step, potentially changing JoinKind
    /// 3. push filter/expression out of JOIN (from pre-filter)
    auto fix_predicate_for_join_logical_step = [&](ActionsDAG filter_dag, const ActionsDAG & side_dag)
    {
        filter_dag = ActionsDAG::merge(side_dag.clone(), std::move(filter_dag));
        auto & outputs = filter_dag.getOutputs();
        outputs.resize(1);
        outputs.insert(outputs.end(), filter_dag.getInputs().begin(), filter_dag.getInputs().end());
        filter_dag.removeUnusedActions();
        return filter_dag;
    };

    if (join_filter_push_down_actions.left_stream_filter_to_push_down)
    {
        if (logical_join)
        {

            join_filter_push_down_actions.left_stream_filter_to_push_down = fix_predicate_for_join_logical_step(
                std::move(*join_filter_push_down_actions.left_stream_filter_to_push_down),
                *logical_join->getExpressionActions().left_pre_join_actions
            );
            join_filter_push_down_actions.left_stream_filter_removes_filter = true;
        }

        const auto & result_name = join_filter_push_down_actions.left_stream_filter_to_push_down->getOutputs()[0]->result_name;
        updated_steps += addNewFilterStepOrThrow(parent_node,
            nodes,
            {std::move(*join_filter_push_down_actions.left_stream_filter_to_push_down), 0, join_filter_push_down_actions.left_stream_filter_removes_filter},
            0 /*child_idx*/,
            false /*update_parent_filter*/);
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
            "Pushed down filter {} to the {} side of join",
            result_name,
            JoinKind::Left);
    }

    if (join_filter_push_down_actions.right_stream_filter_to_push_down && allow_push_down_to_right)
    {
        if (logical_join)
        {

            join_filter_push_down_actions.right_stream_filter_to_push_down = fix_predicate_for_join_logical_step(
                std::move(*join_filter_push_down_actions.right_stream_filter_to_push_down),
                *logical_join->getExpressionActions().right_pre_join_actions
            );
            join_filter_push_down_actions.right_stream_filter_removes_filter = true;
        }

        const auto & result_name = join_filter_push_down_actions.right_stream_filter_to_push_down->getOutputs()[0]->result_name;
        updated_steps += addNewFilterStepOrThrow(parent_node,
            nodes,
            {std::move(*join_filter_push_down_actions.right_stream_filter_to_push_down), 0, join_filter_push_down_actions.right_stream_filter_removes_filter},
            1 /*child_idx*/,
            false /*update_parent_filter*/);
        LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
            "Pushed down filter {} to the {} side of join",
            result_name,
            JoinKind::Right);
    }

    if (updated_steps > 0)
    {
        const auto & filter_column_name = filter->getFilterColumnName();
        auto & filter_expression = filter->getExpression();

        const auto * filter_node = filter_expression.tryFindInOutputs(filter_column_name);
        if (!filter_node && !filter->removesFilterColumn())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                        filter_column_name, filter_expression.dumpDAG());


        /// Filter column was replaced to constant.
        const bool filter_is_constant = filter_node && filter_node->column && isColumnConst(*filter_node->column);

        if (!filter_node || filter_is_constant)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(filter_expression));
        }
        else
        {
            filter->updateInputHeader(child->getOutputHeader());
        }
    }

    return updated_steps;
}

size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter = typeid_cast<FilterStep *>(parent.get());

    if (!filter)
        return 0;

    if (filter->getExpression().hasStatefulFunctions())
        return 0;

    const auto * merging_aggregated = typeid_cast<MergingAggregatedStep *>(child.get());
    const auto * aggregating = typeid_cast<AggregatingStep *>(child.get());

    if (aggregating || merging_aggregated)
    {
        /// If aggregating is GROUPING SETS, and not all the identifiers exist in all
        /// of the grouping sets, we could not push the filter down.
        bool is_grouping_sets = aggregating ? aggregating->isGroupingSets() : merging_aggregated->isGroupingSets();
        if (is_grouping_sets)
        {
            /// Cannot push down filter if type has been changed. MergingAggregated does not change types.
            if (aggregating && aggregating->isGroupByUseNulls())
                return 0;

            const auto & actions = filter->getExpression();
            const auto & filter_node = actions.findInOutputs(filter->getFilterColumnName());

            auto identifiers_in_predicate = findIdentifiersOfNode(&filter_node);

            const auto & grouping_sets = aggregating ? aggregating->getGroupingSetsParamsList() : merging_aggregated->getGroupingSetsParamsList();
            if (!identifiersIsAmongAllGroupingSets(grouping_sets, identifiers_in_predicate))
                return 0;
        }

        const auto & params = aggregating ? aggregating->getParams() : merging_aggregated->getParams();
        const auto & keys = params.keys;
        /** The filter is applied either to aggregation keys or aggregation result
          * (columns under aggregation is not available in outer scope, so we can't have a filter for them).
          * The filter for the aggregation result is not pushed down, so the only valid case is filtering aggregation keys.
          * In case keys are empty, do not push down the filter.
          * Also with empty keys we can have an issue with `empty_result_for_aggregation_by_empty_set`,
          * since we can gen a result row when everything is filtered.
          */
        if (keys.empty())
            return 0;

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, keys))
            return updated_steps;
    }

    if (typeid_cast<CreatingSetsStep *>(child.get()))
    {
        /// CreatingSets does not change header.
        /// We can push down filter and update header.
        ///                       - Something
        /// Filter - CreatingSets - CreatingSet
        ///                       - CreatingSet
        auto input_streams = child->getInputHeaders();
        input_streams.front() = filter->getOutputHeader();
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

        child = std::make_unique<DelayedCreatingSetsStep>(
            filter->getOutputHeader(),
            delayed->detachSets(),
            delayed->getNetworkTransferLimits(),
            delayed->getPreparedSetsCache());

        std::swap(parent, child);
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
        const auto & header = totals_having->getInputHeaders().front();
        for (const auto & column : header)
            if (typeid_cast<const DataTypeAggregateFunction *>(column.type.get()) == nullptr)
                keys.push_back(column.name);

        /// NOTE: this optimization changes TOTALS value. Example:
        ///   `select * from (select y, sum(x) from (
        ///        select number as x, number % 4 as y from numbers(10)
        ///    ) group by y with totals) where y != 2`
        /// Optimization will replace totals row `y, sum(x)` from `(0, 45)` to `(0, 37)`.
        /// It is expected to ok, cause AST optimization `enable_optimize_predicate_expression = 1` also brakes it.
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, keys))
            return updated_steps;
    }

    if (auto * array_join = typeid_cast<ArrayJoinStep *>(child.get()))
    {
        const auto & keys = array_join->getColumns();
        std::unordered_set<std::string_view> keys_set(keys.begin(), keys.end());

        const auto & array_join_header = array_join->getInputHeaders().front();

        Names allowed_inputs;
        for (const auto & column : array_join_header)
            if (!keys_set.contains(column.name))
                allowed_inputs.push_back(column.name);

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto updated_steps = simplePushDownOverStep<DistinctStep>(parent_node, true, nodes, child))
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

    if (typeid_cast<SortingStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader().getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (typeid_cast<CustomMetricLogViewStep *>(child.get()))
    {
        Names allowed_inputs = {"event_date", "event_time", "hostname"};
        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, allowed_inputs))
            return updated_steps;
    }

    if (typeid_cast<CreateSetAndFilterOnTheFlyStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader().getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto * union_step = typeid_cast<UnionStep *>(child.get()))
    {
        /// Union does not change header.
        /// We can push down filter and update header.
        auto union_input_headers = child->getInputHeaders();
        for (auto & input_header : union_input_headers)
            input_header = filter->getOutputHeader();

        ///                - Something
        /// Filter - Union - Something
        ///                - Something

        child = std::make_unique<UnionStep>(union_input_headers, union_step->getMaxThreads());

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
                filter_node.children.front()->step->getOutputHeader(),
                filter->getExpression().clone(),
                filter->getFilterColumnName(),
                filter->removesFilterColumn());
        }

        ///       - Filter - Something
        /// Union - Filter - Something
        ///       - Filter - Something

        return 3;
    }

    if (auto * read_from_merge = typeid_cast<ReadFromMerge *>(child.get()))
    {
        FilterDAGInfo info{filter->getExpression().clone(), filter->getFilterColumnName(), filter->removesFilterColumn()};
        read_from_merge->addFilter(std::move(info));
        std::swap(*parent_node, *child_node);
        return 1;
    }

    return 0;
}

}
