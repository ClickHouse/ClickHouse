#include <Columns/IColumn.h>

#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Interpreters/JoinExpressionActions.h>

#include <DataTypes/DataTypeAggregateFunction.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>

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

namespace
{
/// Returns true if the filter column is present in the output and it is a constant.
/// Returns false if the the filter column removed from the output or it is not a constant.
bool isFilterColumnConst(const FilterStep & filter)
{
    if (filter.removesFilterColumn())
        return false;

    return filter.getOutputHeader()->getByName(filter.getFilterColumnName()).column->isConst();
}

void materializeFilterColumnIfNeededAfterPushDown(FilterStep & filter, const bool was_filter_const_before, const bool is_filter_const_after)
{
    if (was_filter_const_before || !is_filter_const_after)
        return;

    auto & expression = filter.getExpression();
    const auto & filter_node = expression.findInOutputs(filter.getFilterColumnName());
    const auto & non_const_filter_node = expression.materializeNode(filter_node, false);
    expression.addOrReplaceInOutputs(non_const_filter_node);
}
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
    const bool removes_filter = filter->removesFilterColumn();

    const auto & all_inputs = child->getInputHeaders()[child_idx]->getColumnsWithTypeAndName();
    const bool allow_deterministic_functions = !step_changes_the_number_of_rows;
    const bool is_filter_column_const_before = isFilterColumnConst(*filter);
    auto result = expression.splitActionsForFilterPushDown(
        filter_column_name, removes_filter, available_inputs, all_inputs, allow_deterministic_functions);
    if (result)
        materializeFilterColumnIfNeededAfterPushDown(*filter, is_filter_column_const_before, result->is_filter_const_after_push_down);
    return result;
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
    node.step->setStepDescription(*filter);

    child->updateInputHeader(node.step->getOutputHeader(), child_idx);

    if (update_parent_filter)
    {
        if (!filter_node || split_filter.is_filter_const_after_push_down)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            auto new_step = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(expression));
            new_step->setStepDescription(*filter);
            parent = std::move(new_step);
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
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, step_changes_the_number_of_rows, nodes, allowed_inputs))
            return updated_steps;
    }
    return 0;
}

/// Disjoint Set Union (Union-Find) data structure for JoinActionRef.
/// Used to find equivalent expressions in JOIN conditions.
/// For example, if (a = b) and (b = c), then a, b, c belong to the same equivalence class.
class EquivalentJoinKeySet
{
public:
    JoinActionRef findOrAdd(JoinActionRef ref)
    {
        auto it = parent.find(ref);
        if (it == parent.end())
        {
            parent.emplace(ref, ref);
            return ref;
        }

        if (it->second == ref)
            return ref;

        JoinActionRef root = findOrAdd(it->second);
        parent.insert_or_assign(ref, root);
        return root;
    }

    JoinActionRef unite(JoinActionRef a, JoinActionRef b)
    {
        JoinActionRef root_a = findOrAdd(a);
        JoinActionRef root_b = findOrAdd(b);

        if (root_a == root_b)
            return root_a;

        size_t rank_a = rank[root_a];
        size_t rank_b = rank[root_b];

        if (rank_a < rank_b)
            std::swap(root_a, root_b);

        parent.insert_or_assign(root_b, root_a);

        if (rank_a == rank_b)
            ++rank[root_a];

        return root_a;
    }

    bool connected(JoinActionRef a, JoinActionRef b)
    {
        return findOrAdd(a) == findOrAdd(b);
    }

    std::unordered_map<JoinActionRef, std::vector<JoinActionRef>> getClasses()
    {
        std::unordered_map<JoinActionRef, std::vector<JoinActionRef>> classes;
        for (auto & [ref, _] : parent)
            classes[findOrAdd(ref)].push_back(ref);
        return classes;
    }

    std::vector<JoinActionRef> getClass(JoinActionRef ref)
    {
        std::vector<JoinActionRef> res;
        JoinActionRef root = findOrAdd(ref);
        for (auto & [other_ref, _] : parent)
        {
            if (findOrAdd(other_ref) == root)
                res.push_back(other_ref);
        }
        return res;
    }

private:
    std::unordered_map<JoinActionRef, JoinActionRef> parent;
    std::unordered_map<JoinActionRef, size_t> rank;
};

using JoinActionRefPair = std::pair<JoinActionRef, JoinActionRef>;

struct JoinActionRefPairHash
{
    size_t operator()(const JoinActionRefPair & p) const noexcept
    {
        return std::hash<JoinActionRef>{}(p.first) ^ std::hash<JoinActionRef>{}(p.second);
    }
};

std::vector<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator)
{
    std::vector<JoinActionRefPair> joining_keys;
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        auto left_column = lhs.getColumn();
        auto right_column = rhs.getColumn();
        if (!left_column.type->equals(*right_column.type))
            continue;
        joining_keys.emplace_back(lhs, rhs);
    }
    return joining_keys;
}

std::vector<JoinActionRefPair> buildEquialentSetsForJoinStepLogical(
    EquivalentJoinKeySet & equivalent_sets,
    const JoinStepLogical * join_step,
    const std::vector<QueryPlan::Node *> & child_nodes,
    int lookup_depth = 0)
{
    auto join_inputs = join_step->getInputActions();
    auto join_input_it = join_inputs.begin();

    for (const auto * child_node : child_nodes)
    {
        /// Sanity check to avoid expensive computations on too deep plans
        if (lookup_depth >= 32)
            break;

        const auto * child_join = typeid_cast<const JoinStepLogical *>(child_node->step.get());
        if (!child_join)
            continue;
        auto join_strictness = child_join->getJoinOperator().strictness;
        auto join_kind = child_join->getJoinOperator().kind;
        if (join_kind != JoinKind::Inner || (join_strictness != JoinStrictness::All && join_strictness != JoinStrictness::Any))
            continue;

        for (const auto & output : child_join->getOutputActions())
        {
            while (join_input_it != join_inputs.end() && output.getColumnName() != join_input_it->getColumnName())
                ++join_input_it;
            if (join_input_it == join_inputs.end())
                break;
            equivalent_sets.unite(*join_input_it, output);
        }
        buildEquialentSetsForJoinStepLogical(equivalent_sets, child_join, child_node->children, lookup_depth + 1);
    }

    auto joining_keys = getJoiningKeysForJoinStep(join_step->getJoinOperator());
    for (const auto & [lhs, rhs] : joining_keys)
        equivalent_sets.unite(lhs, rhs);
    return joining_keys;
}

static void projectDagInputs(ActionsDAG & actions_dag)
{
    auto & outputs = actions_dag.getOutputs();
    auto existing_outputs = std::ranges::to<std::unordered_set>(outputs);

    for (const auto * node : actions_dag.getInputs())
    {
        if (existing_outputs.contains(node))
            continue;
        outputs.push_back(node);
    }
}

std::optional<ActionsDAG> tryToExtractPartialPredicate(
    const ActionsDAG & original_dag,
    const std::string & filter_name,
    const Names & available_columns);

void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag);

static size_t tryPushDownOverJoinStep(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, QueryPlan::Node * child_node)
{
    auto & parent = parent_node->step;
    QueryPlanStepPtr & child = child_node->step;
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
    if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Full)
        return 0;

    /// PASTE JOIN aligns rows from both sides by position, and pushing filters
    /// to either side may change relative alignment
    if ((table_join_ptr && table_join_ptr->kind() == JoinKind::Paste)
        || (logical_join && logical_join->getJoinOperator().kind == JoinKind::Paste))
        return 0;

    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_left_stream_column_to_right_stream_column;
    std::unordered_map<std::string, ColumnWithTypeAndName> equivalent_right_stream_column_to_left_stream_column;
    std::vector<JoinActionRefPair> equivalent_expressions;

    bool has_single_clause = table_join_ptr && table_join_ptr->getClauses().size() == 1;
    if (has_single_clause && !filled_join)
    {
        const auto & join_clause = table_join_ptr->getClauses()[0];
        size_t key_names_size = join_clause.key_names_left.size();

        for (size_t i = 0; i < key_names_size; ++i)
        {
            const auto & left_table_key_name = join_clause.key_names_left[i];
            const auto & right_table_key_name = join_clause.key_names_right[i];
            const auto & left_table_column = left_stream_input_header->getByName(left_table_key_name);
            const auto & right_table_column = right_stream_input_header->getByName(right_table_key_name);

            if (!left_table_column.type->equals(*right_table_column.type))
                continue;

            equivalent_left_stream_column_to_right_stream_column[left_table_key_name] = right_table_column;
            equivalent_right_stream_column_to_left_stream_column[right_table_key_name] = left_table_column;
        }
    }
    else if (logical_join)
    {
        EquivalentJoinKeySet equivalent_sets;
        equivalent_expressions = buildEquialentSetsForJoinStepLogical(equivalent_sets, logical_join, child_node->children);
        std::unordered_set<JoinActionRefPair, JoinActionRefPairHash> equivalent_expressions_set(equivalent_expressions.begin(), equivalent_expressions.end());
        std::vector<JoinActionRefPair> extra_equivalent_expressions;
        for (const auto & [lhs, rhs] : equivalent_expressions)
        {
            for (const auto & eq_expr : equivalent_sets.getClass(lhs))
            {
                if (!eq_expr.isFromSameActions(lhs) || !eq_expr.fromRight())
                    continue;
                if (equivalent_expressions_set.contains({lhs, eq_expr}))
                    continue;
                equivalent_expressions_set.emplace(lhs, eq_expr);
                extra_equivalent_expressions.emplace_back(lhs, eq_expr);
            }
            for (const auto & eq_expr : equivalent_sets.getClass(rhs))
            {
                if (!eq_expr.isFromSameActions(rhs) || !eq_expr.fromLeft())
                    continue;
                if (equivalent_expressions_set.contains({eq_expr, rhs}))
                    continue;
                equivalent_expressions_set.emplace(eq_expr, rhs);
                extra_equivalent_expressions.emplace_back(eq_expr, rhs);
            }
        }
        equivalent_expressions.append_range(std::move(extra_equivalent_expressions));
    }

    auto get_available_columns_for_filter = [&](bool push_to_left_stream, bool filter_push_down_input_columns_available)
    {
        Names available_input_columns_for_filter;

        if (!filter_push_down_input_columns_available)
            return available_input_columns_for_filter;

        const auto & input_header = push_to_left_stream ? left_stream_input_header : right_stream_input_header;
        const auto & input_columns_names = input_header->getNames();

        for (const auto & name : input_columns_names)
        {
            if (!join_header->has(name))
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

    if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Left)
        right_stream_filter_push_down_input_columns_available = false;
    else if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Right)
        left_stream_filter_push_down_input_columns_available = false;

    /** We disable push down to right table in cases:
      * 1. Right side is already filled. Example: JOIN with Dictionary.
      * 2. ASOF Right join is not supported.
      */
    bool allow_push_down_to_right = join && join->allowPushDownToRight() && table_join_ptr && table_join_ptr->strictness() != JoinStrictness::Asof;
    if (logical_join)
    {
        bool has_logical_lookup = typeid_cast<JoinStepLogicalLookup *>(child_node->children.back()->step.get());
        allow_push_down_to_right = !has_logical_lookup && logical_join->getJoinOperator().strictness != JoinStrictness::Asof;
    }

    if (!allow_push_down_to_right)
        right_stream_filter_push_down_input_columns_available = false;

    Names equivalent_columns_to_push_down;

    std::unordered_map<JoinActionRef, String> equivalent_expressions_alias;
    for (auto & [lhs, rhs] : equivalent_expressions)
    {
        const auto & lhs_original_name = lhs.getColumnName();
        const auto & rhs_original_name = rhs.getColumnName();
        /* If we originally had an OUTER join with join_use_nulls, which altered the types of the inner joined side,
         * and then converted it to INNER because of WHERE conditions that filter out NULLs, we still should preserve nullability.
         *
         * However, the pushed down filter may use a nullable or original column depending on where it comes from.
         *
         * Consider this example:
         *   SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t1.id < 10 AND t2.id > 0
         *
         * Column `t2.id` has a nullable type in the WHERE section, so when we push it through JOIN to the right table,
         * it should remain nullable.
         * In that case filter inherits the toNullable conversion from JoinStepLogical.
         *
         * We can also push down the condition t2.id < 10 which is inferred from the equality condition t1.id = t2.id and t1.id < 10.
         *
         * The `t1.id` is not nullable, as well as in the JOIN ON expression `t1.id = t2.id` are not nullable.
         * So we should replace `t1.id` with the original non-nullable version of `t2.id` column in the `t1.id < 10` expression.
         * In this case we rename this column by adding a unique prefix to it,
         * so we ensure the filter DAG with the replaced column uses the correct one
         * and we can easily distinguish it when merging expressions into it later on.
         */
        auto create_alias = [&](const auto & arg)
        {
            String alias = fmt::format("__filterpushdown_src{}", arg.getColumnName());
            int counter = 0;
            for (; left_stream_input_header->has(alias) || right_stream_input_header->has(alias); ++counter)
                alias = fmt::format("__filterpushdown_src_{}{}", counter, arg.getColumnName());
            return alias;
        };

        bool changes_left_type = logical_join && logical_join->typeChangingSides().contains(JoinTableSide::Left);
        bool changes_right_type = logical_join && logical_join->typeChangingSides().contains(JoinTableSide::Right);

        auto lhs_column = lhs.getColumn();
        auto rhs_column = rhs.getColumn();

        if (changes_left_type)
        {
            auto alias = create_alias(lhs);
            lhs_column.name = alias;
            equivalent_expressions_alias[lhs] = alias;
        }
        if (changes_right_type)
        {
            auto alias = create_alias(rhs);
            rhs_column.name = alias;
            equivalent_expressions_alias[rhs] = alias;
        }

        if (!changes_left_type)
            equivalent_left_stream_column_to_right_stream_column[lhs_original_name] = rhs_column;
        if (!changes_right_type)
            equivalent_right_stream_column_to_left_stream_column[rhs_original_name] = lhs_column;
    }

    Names left_stream_available_columns_to_push_down = get_available_columns_for_filter(true /*push_to_left_stream*/, left_stream_filter_push_down_input_columns_available);
    Names right_stream_available_columns_to_push_down = get_available_columns_for_filter(false /*push_to_left_stream*/, right_stream_filter_push_down_input_columns_available);

    if (left_stream_filter_push_down_input_columns_available)
    {
        for (const auto & [name, _] : equivalent_left_stream_column_to_right_stream_column)
            equivalent_columns_to_push_down.push_back(name);
    }
    else if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Right && logical_join->getJoinOperator().strictness == JoinStrictness::Semi)
    {
        if (!logical_join->typeChangingSides().contains(JoinTableSide::Left))
        {
            /// In this case we can also push down to left side of JOIN using equivalent sets.
            for (const auto & [name, _] : equivalent_left_stream_column_to_right_stream_column)
                equivalent_columns_to_push_down.push_back(name);
        }
    }

    if (right_stream_filter_push_down_input_columns_available)
    {
        for (const auto & [name, _] : equivalent_right_stream_column_to_left_stream_column)
            equivalent_columns_to_push_down.push_back(name);
    }
    else if (logical_join && logical_join->getJoinOperator().kind == JoinKind::Left && logical_join->getJoinOperator().strictness == JoinStrictness::Semi)
    {
        if (!logical_join->typeChangingSides().contains(JoinTableSide::Right))
        {
            /// In this case we can also push down to right side of JOIN using equivalent sets.
            for (const auto & [name, _] : equivalent_right_stream_column_to_left_stream_column)
                equivalent_columns_to_push_down.push_back(name);
        }
    }

    const bool is_filter_column_const_before = isFilterColumnConst(*filter);
    auto join_filter_push_down_actions = filter->getExpression().splitActionsForJOINFilterPushDown(
        filter->getFilterColumnName(),
        filter->removesFilterColumn(),
        left_stream_available_columns_to_push_down,
        *left_stream_input_header,
        right_stream_available_columns_to_push_down,
        *right_stream_input_header,
        equivalent_columns_to_push_down,
        equivalent_left_stream_column_to_right_stream_column,
        equivalent_right_stream_column_to_left_stream_column);

    materializeFilterColumnIfNeededAfterPushDown(
        *filter, is_filter_column_const_before, join_filter_push_down_actions.is_filter_const_after_all_push_downs);

    size_t updated_steps = 0;

    /// For the logical join step, we need to merge pre-join actions to filter dag.
    /// TODO: this could be refactored and replaced with "expression pushdown to JOIN" optimizations which
    /// 1. push filter/expression into JOIN (as post-filter)
    /// 2. move filter within JOIN step, potentially changing JoinKind
    /// 3. push filter/expression out of JOIN (from pre-filter)
    auto fix_predicate_for_join_logical_step = [&](ActionsDAG filter_dag, ActionsDAG pre_filter_dag)
    {
        projectDagInputs(pre_filter_dag);
        filter_dag = ActionsDAG::merge(std::move(pre_filter_dag), std::move(filter_dag));
        auto & outputs = filter_dag.getOutputs();
        outputs.resize(1);

        projectDagInputs(filter_dag);
        filter_dag.removeUnusedActions();
        return filter_dag;
    };

    /// Filter DAG may use columns with types changed by JOIN due to join_use_nulls.
    /// In that case, the actions DAG in JoinStepLogical will have toNullable conversions.
    /// The filter applied after JOIN uses those columns with altered types, so when we push it down through JOIN,
    /// we should also apply the required conversions.
    /// Here we extract required expressions from JoinStepLogical actions DAG and apply them to the filter DAG that expects them to be calculated.
    /// In general, it could be not only toNullable, but arbitrary expressions, which may be the case once expression pushdown to JOIN is implemented.
    auto get_required_pre_actions = [&](const auto & join_actions, const auto & filter_dag_inputs)
    {
        /// In case of duplicate names resolve them in corresponding order
        std::unordered_map<std::string_view, size_t> filter_dag_inputs_map;
        for (const auto * node : filter_dag_inputs)
            filter_dag_inputs_map[node->result_name]++;

        std::vector<JoinActionRef> required_actions;
        for (const auto & join_action : join_actions)
        {
            auto it = filter_dag_inputs_map.find(join_action.getColumnName());
            if (it != filter_dag_inputs_map.end() && it->second > 0)
            {
                /// If for any reason we have column with same name multiple times,
                /// return it as many times as it is needed in filter_dag_inputs.
                it->second--;
                required_actions.push_back(join_action);
            }
        }
        return required_actions;
    };

    if (join_filter_push_down_actions.left_stream_filter_to_push_down)
    {
        if (logical_join)
        {
            const auto & filter_dag_inputs = join_filter_push_down_actions.left_stream_filter_to_push_down->getInputs();
            std::vector<JoinActionRef> required_actions_from_join = get_required_pre_actions(logical_join->getOutputActions(), filter_dag_inputs);
            for (auto [lhs, _] : equivalent_expressions)
            {
                if (auto it = equivalent_expressions_alias.find(lhs); it != equivalent_expressions_alias.end())
                    lhs = JoinActionRef::transform({lhs}, [&](ActionsDAG & dag, auto && args) { return &dag.addAlias(*args.at(0), it->second); });
                required_actions_from_join.push_back(lhs);
            }
            auto pre_filter_dag = JoinExpressionActions::getSubDAG(required_actions_from_join);
            *join_filter_push_down_actions.left_stream_filter_to_push_down = fix_predicate_for_join_logical_step(
                std::move(*join_filter_push_down_actions.left_stream_filter_to_push_down), std::move(pre_filter_dag));
            join_filter_push_down_actions.left_stream_filter_removes_filter = true;
        }

        const auto & result_name = join_filter_push_down_actions.left_stream_filter_to_push_down->getOutputs()[0]->result_name;
        updated_steps += addNewFilterStepOrThrow(
            parent_node,
            nodes,
            {std::move(*join_filter_push_down_actions.left_stream_filter_to_push_down),
             0,
             join_filter_push_down_actions.left_stream_filter_removes_filter,
             false},
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
            const auto & filter_dag_inputs = join_filter_push_down_actions.right_stream_filter_to_push_down->getInputs();
            std::vector<JoinActionRef> required_actions_from_join = get_required_pre_actions(logical_join->getOutputActions(), filter_dag_inputs);
            for (auto [_, rhs] : equivalent_expressions)
            {
                if (auto it = equivalent_expressions_alias.find(rhs); it != equivalent_expressions_alias.end())
                    rhs = JoinActionRef::transform({rhs}, [&](ActionsDAG & dag, auto && args) { return &dag.addAlias(*args.at(0), it->second); });
                required_actions_from_join.push_back(rhs);
            }
            auto pre_filter_dag = JoinExpressionActions::getSubDAG(required_actions_from_join);
            *join_filter_push_down_actions.right_stream_filter_to_push_down = fix_predicate_for_join_logical_step(
                std::move(*join_filter_push_down_actions.right_stream_filter_to_push_down), std::move(pre_filter_dag));
            join_filter_push_down_actions.right_stream_filter_removes_filter = true;
        }

        const auto & result_name = join_filter_push_down_actions.right_stream_filter_to_push_down->getOutputs()[0]->result_name;
        updated_steps += addNewFilterStepOrThrow(
            parent_node,
            nodes,
            {std::move(*join_filter_push_down_actions.right_stream_filter_to_push_down),
             0,
             join_filter_push_down_actions.right_stream_filter_removes_filter,
             false},
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
        if (!filter_node || join_filter_push_down_actions.is_filter_const_after_all_push_downs)
        {
            /// This means that all predicates of filter were pushed down.
            /// Replace current actions to expression, as we don't need to filter anything.
            parent = std::make_unique<ExpressionStep>(child->getOutputHeader(), std::move(filter_expression));
            filter = nullptr;
        }
        else
        {
            filter->updateInputHeader(child->getOutputHeader());
        }
    }

    const bool disj_pushdown_enabled = (join && join->useJoinDisjunctionsPushDown()) || (logical_join && logical_join->getSettings().use_join_disjunctions_push_down);
    if (filter && disj_pushdown_enabled)
    {
        if ((join && join->isDisjunctionsOptimizationApplied()) ||
            (logical_join && logical_join->isDisjunctionsOptimizationApplied()) ||
            (filled_join && filled_join->isDisjunctionsOptimizationApplied()))
        {
            return updated_steps;
        }

        {
            auto left_partial_filter_dag = tryToExtractPartialPredicate(filter->getExpression(), filter->getFilterColumnName(), left_stream_available_columns_to_push_down);
            if (left_partial_filter_dag.has_value())
            {
                const auto partial_predicate_column_name = left_partial_filter_dag->getOutputs().front()->result_name;
                addFilterOnTop(*child_node, 0, nodes, std::move(*left_partial_filter_dag));
                ++updated_steps;
                LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
                    "Pushed down partial filter {} to the {} side of join",
                    partial_predicate_column_name,
                    JoinKind::Left);
            }
        }

        {
            auto right_partial_filter_dag = tryToExtractPartialPredicate(filter->getExpression(), filter->getFilterColumnName(), right_stream_available_columns_to_push_down);
            if (right_partial_filter_dag.has_value())
            {
                const auto partial_predicate_column_name = right_partial_filter_dag->getOutputs().front()->result_name;
                addFilterOnTop(*child_node, 1, nodes, std::move(*right_partial_filter_dag));
                ++updated_steps;
                LOG_DEBUG(&Poco::Logger::get("QueryPlanOptimizations"),
                    "Pushed down partial filter {} to the {} side of join",
                    partial_predicate_column_name,
                    JoinKind::Right);
            }
        }

        if (join)
            join->setDisjunctionsOptimizationApplied(true);
        if (logical_join)
            logical_join->setDisjunctionsOptimizationApplied(true);
        if (filled_join)
            filled_join->setDisjunctionsOptimizationApplied(true);
    }

    return updated_steps;
}

size_t tryPushDownFilter(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
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
        for (const auto & column : *header)
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
        for (const auto & column : *array_join_header)
            if (!keys_set.contains(column.name))
                allowed_inputs.push_back(column.name);

        if (auto updated_steps = tryAddNewFilterStep(parent_node, true, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto updated_steps = simplePushDownOverStep<DistinctStep>(parent_node, true, nodes, child))
        return updated_steps;

    if (auto updated_steps = simplePushDownOverStep<BuildRuntimeFilterStep>(parent_node, true, nodes, child))
        return updated_steps;

    if (auto updated_steps = tryPushDownOverJoinStep(parent_node, nodes, child_node))
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
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (typeid_cast<CreateSetAndFilterOnTheFlyStep *>(child.get()))
    {
        Names allowed_inputs = child->getOutputHeader()->getNames();
        if (auto updated_steps = tryAddNewFilterStep(parent_node, false, nodes, allowed_inputs))
            return updated_steps;
    }

    if (auto * union_step = typeid_cast<UnionStep *>(child.get()))
    {
        /// Union does not change header.
        /// We can push down filter and update header.
        auto union_input_headers = child->getInputHeaders();
        auto expected_output = filter->getOutputHeader();

        for (auto & input_header : union_input_headers)
            input_header = expected_output;

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

    if (auto * parallel_replicas_local_plan = typeid_cast<ReadFromLocalParallelReplicaStep *>(child.get()))
    {
        if (!settings.parallel_replicas_filter_pushdown)
            return 0;

        // actual push down will be done when plan for local parallel replica will be optimized
        FilterDAGInfo info{filter->getExpression().clone(), filter->getFilterColumnName(), filter->removesFilterColumn()};
        parallel_replicas_local_plan->addFilter(std::move(info));
        std::swap(*parent_node, *child_node);
        return 1;
    }

    if (auto * read_from_merge = typeid_cast<ReadFromMerge *>(child.get()))
    {
        FilterDAGInfo info{filter->getExpression().clone(), filter->getFilterColumnName(), filter->removesFilterColumn()};
        read_from_merge->addFilter(std::move(info));
        std::swap(*parent_node, *child_node);
        return 1;
    }

    if (auto updated_steps = simplePushDownOverStep<CommonSubplanStep>(parent_node, false, nodes, child))
        return updated_steps;

    return 0;
}

}
