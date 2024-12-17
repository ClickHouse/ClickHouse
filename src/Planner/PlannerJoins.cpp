#include <Planner/PlannerJoins.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageDictionary.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <Analyzer/Utils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>

#include <Dictionaries/IDictionary.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/DirectJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/PasteJoin.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <Core/Settings.h>
#include <Core/ServerSettings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_join_condition;
    extern const SettingsBool collect_hash_table_stats_during_joins;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsBool join_use_nulls;
    extern const SettingsUInt64 max_size_to_preallocate_for_joins;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

void JoinClause::dump(WriteBuffer & buffer) const
{
    auto dump_dag_nodes = [&](const ActionsDAG::NodeRawConstPtrs & dag_nodes)
    {
        String dag_nodes_dump;

        if (!dag_nodes.empty())
        {
            for (const auto & dag_node : dag_nodes)
            {
                dag_nodes_dump += dag_node->result_name;
                dag_nodes_dump += " ";
                dag_nodes_dump += dag_node->result_type->getName();
                dag_nodes_dump += ", ";
            }

            dag_nodes_dump.pop_back();
            dag_nodes_dump.pop_back();
        }

        return dag_nodes_dump;
    };

    buffer << "left_key_nodes: " << dump_dag_nodes(left_key_nodes);
    buffer << " right_key_nodes: " << dump_dag_nodes(right_key_nodes);

    if (!left_filter_condition_nodes.empty())
        buffer << " left_condition_nodes: " + dump_dag_nodes(left_filter_condition_nodes);

    if (!right_filter_condition_nodes.empty())
        buffer << " right_condition_nodes: " + dump_dag_nodes(right_filter_condition_nodes);

    if (!asof_conditions.empty())
    {
        buffer << " asof_conditions: ";
        size_t asof_conditions_size = asof_conditions.size();

        for (size_t i = 0; i < asof_conditions_size; ++i)
        {
            const auto & asof_condition = asof_conditions[i];

            buffer << " key_index: " << asof_condition.key_index;
            buffer << " inequality: " << toString(asof_condition.asof_inequality);

            if (i + 1 != asof_conditions_size)
                buffer << ',';
        }
    }
}

String JoinClause::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}

namespace
{

using TableExpressionSet = std::unordered_set<const IQueryTreeNode *>;

TableExpressionSet extractTableExpressionsSet(const QueryTreeNodePtr & node)
{
    TableExpressionSet res;
    for (const auto & expr : extractTableExpressions(node, true))
        res.insert(expr.get());

    return res;
}

std::set<JoinTableSide> extractJoinTableSidesFromExpression(//const ActionsDAG::Node * expression_root_node,
    const IQueryTreeNode * expression_root_node,
    const TableExpressionSet & left_table_expressions,
    const TableExpressionSet & right_table_expressions,
    const JoinNode & join_node)
{
    std::set<JoinTableSide> table_sides;
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(expression_root_node);

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (const auto * function_node = node_to_process->as<FunctionNode>())
        {
            for (const auto & child : function_node->getArguments())
                nodes_to_process.push_back(child.get());

            continue;
        }

        const auto * column_node = node_to_process->as<ColumnNode>();
        if (!column_node)
            continue;

        const auto & input_name = column_node->getColumnName();
        const auto * column_source = column_node->getColumnSource().get();
        if (!column_source)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No source for column {} in JOIN {}", input_name, join_node.formatASTForErrorMessage());

        bool is_column_from_left_expr = left_table_expressions.contains(column_source);
        bool is_column_from_right_expr = right_table_expressions.contains(column_source);

        if (!is_column_from_left_expr && !is_column_from_right_expr)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} actions has column {} that do not exist in left {} or right {} table expression columns",
                join_node.formatASTForErrorMessage(),
                column_source->formatASTForErrorMessage(),
                join_node.getLeftTableExpression()->formatASTForErrorMessage(),
                join_node.getRightTableExpression()->formatASTForErrorMessage());

        auto input_table_side = is_column_from_left_expr ? JoinTableSide::Left : JoinTableSide::Right;
        table_sides.insert(input_table_side);
    }

    return table_sides;
}

const ActionsDAG::Node * appendExpression(
    ActionsDAG & dag,
    const QueryTreeNodePtr & expression,
    const PlannerContextPtr & planner_context,
    const JoinNode & join_node)
{
    PlannerActionsVisitor join_expression_visitor(planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(dag, expression);
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JOIN {} ON clause contains multiple expressions",
            join_node.formatASTForErrorMessage());

    return join_expression_dag_node_raw_pointers[0];
}

void buildJoinClause(
    ActionsDAG & left_dag,
    ActionsDAG & right_dag,
    ActionsDAG & mixed_dag,
    const PlannerContextPtr & planner_context,
    const QueryTreeNodePtr & join_expression,
    const TableExpressionSet & left_table_expressions,
    const TableExpressionSet & right_table_expressions,
    const JoinNode & join_node,
    JoinClause & join_clause)
{
    std::string function_name;
    auto * function_node = join_expression->as<FunctionNode>();
    if (function_node)
        function_name = function_node->getFunction()->getName();

    /// For 'and' function go into children
    if (function_name == "and")
    {
        for (const auto & child : function_node->getArguments())
        {
            buildJoinClause(
                left_dag,
                right_dag,
                mixed_dag,
                planner_context,
                child,
                left_table_expressions,
                right_table_expressions,
                join_node,
                join_clause);
        }

        return;
    }

    auto asof_inequality = getASOFJoinInequality(function_name);
    bool is_asof_join_inequality = join_node.getStrictness() == JoinStrictness::Asof && asof_inequality != ASOFJoinInequality::None;

    if (function_name == "equals" || function_name == "isNotDistinctFrom" || is_asof_join_inequality)
    {
        const auto left_child = function_node->getArguments().getNodes().at(0);
        const auto right_child = function_node->getArguments().getNodes().at(1);

        auto left_expression_sides = extractJoinTableSidesFromExpression(left_child.get(),
            left_table_expressions,
            right_table_expressions,
            join_node);

        auto right_expression_sides = extractJoinTableSidesFromExpression(right_child.get(),
            left_table_expressions,
            right_table_expressions,
            join_node);

        if (left_expression_sides.empty() && right_expression_sides.empty())
        {
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} ON expression expected non-empty left and right table expressions",
                join_node.formatASTForErrorMessage());
        }
        else if (left_expression_sides.size() == 1 && right_expression_sides.empty())
        {
            auto expression_side = *left_expression_sides.begin();
            auto & dag =  expression_side == JoinTableSide::Left ? left_dag : right_dag;
            const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
            join_clause.addCondition(expression_side, node);

        }
        else if (left_expression_sides.empty() && right_expression_sides.size() == 1)
        {
            auto expression_side = *right_expression_sides.begin();
            auto & dag =  expression_side == JoinTableSide::Left ? left_dag : right_dag;
            const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
            join_clause.addCondition(expression_side, node);

        }
        else if (left_expression_sides.size() == 1 && right_expression_sides.size() == 1)
        {
            auto left_expression_side = *left_expression_sides.begin();
            auto right_expression_side = *right_expression_sides.begin();

            if (left_expression_side != right_expression_side)
            {
                auto left_key = left_child;
                auto right_key = right_child;

                if (left_expression_side == JoinTableSide::Right)
                {
                    left_key = right_child;
                    right_key = left_child;
                    asof_inequality = reverseASOFJoinInequality(asof_inequality);
                }

                const auto * left_node = appendExpression(left_dag, left_key, planner_context, join_node);
                const auto * right_node = appendExpression(right_dag, right_key, planner_context, join_node);

                if (is_asof_join_inequality)
                {
                    if (join_clause.hasASOF())
                    {
                        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                            "JOIN {} ASOF JOIN expects exactly one inequality in ON section",
                            join_node.formatASTForErrorMessage());
                    }

                    join_clause.addASOFKey(left_node, right_node, asof_inequality);
                }
                else
                {
                    bool null_safe_comparison = function_name == "isNotDistinctFrom";
                    join_clause.addKey(left_node, right_node, null_safe_comparison);
                }
            }
            else
            {
                auto & dag = left_expression_side == JoinTableSide::Left ? left_dag : right_dag;
                const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
                join_clause.addCondition(left_expression_side, node);
            }
        }
        else
        {
            auto support_mixed_join_condition = planner_context->getQueryContext()->getSettingsRef()[Setting::allow_experimental_join_condition];
            auto join_use_nulls = planner_context->getQueryContext()->getSettingsRef()[Setting::join_use_nulls];
            /// If join_use_nulls = true, the columns' nullability will be changed later which make this expression not right.
            if (support_mixed_join_condition && !join_use_nulls)
            {
                /// expression involves both tables.
                /// `expr1(left.col1, right.col2) == expr2(left.col3, right.col4)`
                const auto * node = appendExpression(mixed_dag, join_expression, planner_context, join_node);
                join_clause.addMixedCondition(node);
            }
            else
            {
                throw Exception(
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                    "JOIN {} join expression contains column from left and right table, you may try experimental support of this feature by `SET allow_experimental_join_condition = 1`",
                    join_node.formatASTForErrorMessage());
            }
        }

    }
    else
    {
        auto expression_sides = extractJoinTableSidesFromExpression(join_expression.get(),
            left_table_expressions,
            right_table_expressions,
            join_node);
        // expression_sides.empty() = true, the expression is constant
        if (expression_sides.empty() || expression_sides.size() == 1)
        {
            auto expression_side = expression_sides.empty() ? JoinTableSide::Right : *expression_sides.begin();
            auto & dag = expression_side == JoinTableSide::Left ? left_dag : right_dag;
            const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
            join_clause.addCondition(expression_side, node);
        }
        else
        {
            auto support_mixed_join_condition = planner_context->getQueryContext()->getSettingsRef()[Setting::allow_experimental_join_condition];
            auto join_use_nulls = planner_context->getQueryContext()->getSettingsRef()[Setting::join_use_nulls];
            /// If join_use_nulls = true, the columns' nullability will be changed later which make this expression not right.
            if (support_mixed_join_condition && !join_use_nulls)
            {
                /// expression involves both tables.
                const auto * node = appendExpression(mixed_dag, join_expression, planner_context, join_node);
                join_clause.addMixedCondition(node);
            }
            else
            {
                throw Exception(
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                    "JOIN {} join expression contains column from left and right table, you may try experimental support of this feature by `SET allow_experimental_join_condition = 1`",
                    join_node.formatASTForErrorMessage());
            }
        }
    }
}

JoinClausesAndActions buildJoinClausesAndActions(
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    ActionsDAG left_join_actions(left_table_expression_columns);
    ActionsDAG right_join_actions(right_table_expression_columns);
    ColumnsWithTypeAndName mixed_table_expression_columns;
    for (const auto & left_column : left_table_expression_columns)
    {
        mixed_table_expression_columns.push_back(left_column);
    }
    for (const auto & right_column : right_table_expression_columns)
    {
        mixed_table_expression_columns.push_back(right_column);
    }
    ActionsDAG mixed_join_actions(mixed_table_expression_columns);

    /** It is possible to have constant value in JOIN ON section, that we need to ignore during DAG construction.
      * If we do not ignore it, this function will be replaced by underlying constant.
      * For example ASOF JOIN does not support JOIN with constants, and we should process it like ordinary JOIN.
      *
      * Example: SELECT * FROM (SELECT 1 AS id, 1 AS value) AS t1 ASOF LEFT JOIN (SELECT 1 AS id, 1 AS value) AS t2
      * ON (t1.id = t2.id) AND 1 != 1 AND (t1.value >= t1.value);
      */
    auto join_expression = join_node.getJoinExpression();
    auto * constant_join_expression = join_expression->as<ConstantNode>();

    if (constant_join_expression && constant_join_expression->hasSourceExpression())
        join_expression = constant_join_expression->getSourceExpression();

    auto * function_node = join_expression->as<FunctionNode>();
    if (!function_node)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            join_node.formatASTForErrorMessage());

    size_t left_table_expression_columns_size = left_table_expression_columns.size();

    Names join_left_actions_names;
    join_left_actions_names.reserve(left_table_expression_columns_size);

    NameSet join_left_actions_names_set;
    join_left_actions_names_set.reserve(left_table_expression_columns_size);

    for (const auto & left_table_expression_column : left_table_expression_columns)
    {
        join_left_actions_names.push_back(left_table_expression_column.name);
        join_left_actions_names_set.insert(left_table_expression_column.name);
    }

    size_t right_table_expression_columns_size = right_table_expression_columns.size();

    Names join_right_actions_names;
    join_right_actions_names.reserve(right_table_expression_columns_size);

    NameSet join_right_actions_names_set;
    join_right_actions_names_set.reserve(right_table_expression_columns_size);

    for (const auto & right_table_expression_column : right_table_expression_columns)
    {
        join_right_actions_names.push_back(right_table_expression_column.name);
        join_right_actions_names_set.insert(right_table_expression_column.name);
    }

    auto join_left_table_expressions = extractTableExpressionsSet(join_node.getLeftTableExpression());
    auto join_right_table_expressions = extractTableExpressionsSet(join_node.getRightTableExpression());

    JoinClausesAndActions result;

    bool is_inequal_join = false;
    const auto & function_name = function_node->getFunction()->getName();
    if (function_name == "or")
    {
        for (const auto & child : function_node->getArguments())
        {
            result.join_clauses.emplace_back();

            buildJoinClause(
                left_join_actions,
                right_join_actions,
                mixed_join_actions,
                planner_context,
                child,
                join_left_table_expressions,
                join_right_table_expressions,
                join_node,
                result.join_clauses.back());
            is_inequal_join |= !result.join_clauses.back().getMixedFilterConditionNodes().empty();
        }
    }
    else
    {
        result.join_clauses.emplace_back();

        buildJoinClause(
                left_join_actions,
                right_join_actions,
                mixed_join_actions,
                planner_context,
                join_expression,
                join_left_table_expressions,
                join_right_table_expressions,
                join_node,
                result.join_clauses.back());
        is_inequal_join |= !result.join_clauses.back().getMixedFilterConditionNodes().empty();
    }

    auto and_function = FunctionFactory::instance().get("and", planner_context->getQueryContext());

    auto add_necessary_name_if_needed = [&](JoinTableSide join_table_side, const String & name)
    {
        auto & necessary_names = join_table_side == JoinTableSide::Left ? join_left_actions_names : join_right_actions_names;
        auto & necessary_names_set = join_table_side == JoinTableSide::Left ? join_left_actions_names_set : join_right_actions_names_set;

        auto [_, inserted] = necessary_names_set.emplace(name);
        if (inserted)
            necessary_names.push_back(name);
    };

    bool is_join_with_special_storage = false;
    if (const auto * right_table_node = join_node.getRightTableExpression()->as<TableNode>())
    {
        is_join_with_special_storage = dynamic_cast<const StorageJoin *>(right_table_node->getStorage().get());
    }

    for (auto & join_clause : result.join_clauses)
    {
        const auto & left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
        if (!left_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;
            if (left_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &left_join_actions.addFunction(and_function, left_filter_condition_nodes, {});
            else
                dag_filter_condition_node = left_filter_condition_nodes[0];

            join_clause.getLeftFilterConditionNodes() = {dag_filter_condition_node};
            left_join_actions.addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Left, dag_filter_condition_node->result_name);
        }

        const auto & right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
        if (!right_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (right_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &right_join_actions.addFunction(and_function, right_filter_condition_nodes, {});
            else
                dag_filter_condition_node = right_filter_condition_nodes[0];

            join_clause.getRightFilterConditionNodes() = {dag_filter_condition_node};
            right_join_actions.addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Right, dag_filter_condition_node->result_name);
        }

        assert(join_clause.getLeftKeyNodes().size() == join_clause.getRightKeyNodes().size());
        size_t join_clause_key_nodes_size = join_clause.getLeftKeyNodes().size();

        if (join_clause_key_nodes_size == 0)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in {}",
                join_node.formatASTForErrorMessage());

        for (size_t i = 0; i < join_clause_key_nodes_size; ++i)
        {
            auto & left_key_node = join_clause.getLeftKeyNodes()[i];
            auto & right_key_node = join_clause.getRightKeyNodes()[i];

            if (!left_key_node->result_type->equals(*right_key_node->result_type))
            {
                DataTypePtr common_type;

                try
                {
                    common_type = getLeastSupertype(DataTypes{left_key_node->result_type, right_key_node->result_type});
                }
                catch (Exception & ex)
                {
                    ex.addMessage("JOIN {} cannot infer common type in ON section for keys. Left key {} type {}. Right key {} type {}",
                        join_node.formatASTForErrorMessage(),
                        left_key_node->result_name,
                        left_key_node->result_type->getName(),
                        right_key_node->result_name,
                        right_key_node->result_type->getName());
                    throw;
                }

                if (!left_key_node->result_type->equals(*common_type))
                    left_key_node = &left_join_actions.addCast(*left_key_node, common_type, {});

                if (!is_join_with_special_storage && !right_key_node->result_type->equals(*common_type))
                    right_key_node = &right_join_actions.addCast(*right_key_node, common_type, {});
            }

            if (join_clause.isNullsafeCompareKey(i) && left_key_node->result_type->isNullable() && right_key_node->result_type->isNullable())
            {
                /**
                  * In case of null-safe comparison (a IS NOT DISTINCT FROM b),
                  * we need to wrap keys with a non-nullable type.
                  * The type `tuple` can be used for this purpose,
                  * because value tuple(NULL) is not NULL itself (moreover it has type Tuple(Nullable(T) which is not Nullable).
                  * Thus, join algorithm will match keys with values tuple(NULL).
                  * Example:
                  *   SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.b
                  * This will be semantically transformed to:
                  *   SELECT * FROM t1 JOIN t2 ON tuple(t1.a) == tuple(t2.b)
                  */
                auto wrap_nullsafe_function = FunctionFactory::instance().get("tuple", planner_context->getQueryContext());
                left_key_node = &left_join_actions.addFunction(wrap_nullsafe_function, {left_key_node}, {});
                right_key_node = &right_join_actions.addFunction(wrap_nullsafe_function, {right_key_node}, {});
            }

            left_join_actions.addOrReplaceInOutputs(*left_key_node);
            right_join_actions.addOrReplaceInOutputs(*right_key_node);

            add_necessary_name_if_needed(JoinTableSide::Left, left_key_node->result_name);
            add_necessary_name_if_needed(JoinTableSide::Right, right_key_node->result_name);
        }
    }

    result.left_join_expressions_actions = left_join_actions.clone();
    result.left_join_tmp_expression_actions = std::move(left_join_actions);
    result.left_join_expressions_actions.removeUnusedActions(join_left_actions_names);
    result.right_join_expressions_actions = right_join_actions.clone();
    result.right_join_tmp_expression_actions = std::move(right_join_actions);
    result.right_join_expressions_actions.removeUnusedActions(join_right_actions_names);

    if (is_inequal_join)
    {
        /// In case of multiple disjuncts and any inequal join condition, we need to build full join on expression actions.
        /// So, for each column, we recalculate the value of the whole expression from JOIN ON to check if rows should be joined.
        if (result.join_clauses.size() > 1)
        {
            ActionsDAG mixed_join_expressions_actions(mixed_table_expression_columns);
            PlannerActionsVisitor join_expression_visitor(planner_context);
            auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(mixed_join_expressions_actions, join_expression);
            if (join_expression_dag_node_raw_pointers.size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "JOIN {} ON clause contains multiple expressions", join_node.formatASTForErrorMessage());

            mixed_join_expressions_actions.addOrReplaceInOutputs(*join_expression_dag_node_raw_pointers[0]);
            Names required_names{join_expression_dag_node_raw_pointers[0]->result_name};
            mixed_join_expressions_actions.removeUnusedActions(required_names);
            result.mixed_join_expressions_actions = std::move(mixed_join_expressions_actions);
        }
        else
        {
            const auto & join_clause = result.join_clauses.front();
            const auto & mixed_filter_condition_nodes = join_clause.getMixedFilterConditionNodes();
            auto mixed_join_expressions_actions = ActionsDAG::buildFilterActionsDAG(mixed_filter_condition_nodes, {}, true);
            result.mixed_join_expressions_actions = std::move(mixed_join_expressions_actions);
        }
        auto outputs = result.mixed_join_expressions_actions->getOutputs();
        if (outputs.size() != 1)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Only one output is expected, got: {}", result.mixed_join_expressions_actions->dumpDAG());
        }
        auto output_type = removeNullable(outputs[0]->result_type);
        WhichDataType which_type(output_type);
        if (!which_type.isUInt8())
        {
            DataTypePtr uint8_ty = std::make_shared<DataTypeUInt8>();
            auto true_col = ColumnWithTypeAndName(uint8_ty->createColumnConst(1, 1), uint8_ty, "true");
            const auto * true_node = &result.mixed_join_expressions_actions->addColumn(true_col);
            result.mixed_join_expressions_actions = ActionsDAG::buildFilterActionsDAG({outputs[0], true_node});
        }
    }

    return result;
}

}

JoinClausesAndActions buildJoinClausesAndActions(
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const QueryTreeNodePtr & join_node,
    const PlannerContextPtr & planner_context)
{
    auto & join_node_typed = join_node->as<JoinNode &>();
    if (!join_node_typed.isOnJoinExpression())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JOIN {} join does not have ON section",
            join_node_typed.formatASTForErrorMessage());

    return buildJoinClausesAndActions(left_table_expression_columns, right_table_expression_columns, join_node_typed, planner_context);
}

std::optional<bool> tryExtractConstantFromJoinNode(const QueryTreeNodePtr & join_node)
{
    auto & join_node_typed = join_node->as<JoinNode &>();
    if (!join_node_typed.getJoinExpression())
        return {};

    return tryExtractConstantFromConditionNode(join_node_typed.getJoinExpression());
}

namespace
{

void trySetStorageInTableJoin(const QueryTreeNodePtr & table_expression, std::shared_ptr<TableJoin> & table_join)
{
    StoragePtr storage;

    if (auto * table_node = table_expression->as<TableNode>())
        storage = table_node->getStorage();
    else if (auto * table_function = table_expression->as<TableFunctionNode>())
        storage = table_function->getStorage();

    auto storage_join = std::dynamic_pointer_cast<StorageJoin>(storage);
    if (storage_join)
    {
        table_join->setStorageJoin(storage_join);
        return;
    }

    if (!table_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT) && !table_join->isEnabledAlgorithm(JoinAlgorithm::DEFAULT))
        return;

    if (auto storage_dictionary = std::dynamic_pointer_cast<StorageDictionary>(storage);
        storage_dictionary && storage_dictionary->getDictionary()->getSpecialKeyType() != DictionarySpecialKeyType::Range)
        table_join->setStorageJoin(std::dynamic_pointer_cast<const IKeyValueEntity>(storage_dictionary->getDictionary()));
    else if (auto storage_key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage); storage_key_value)
        table_join->setStorageJoin(storage_key_value);
}

std::shared_ptr<DirectKeyValueJoin> tryDirectJoin(const std::shared_ptr<TableJoin> & table_join,
    const QueryTreeNodePtr & right_table_expression,
    const Block & right_table_expression_header,
    const PlannerContextPtr & planner_context)
{
    if (!table_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT))
        return {};

    auto storage = table_join->getStorageKeyValue();
    if (!storage)
        return {};

    bool allowed_inner = isInner(table_join->kind()) && table_join->strictness() == JoinStrictness::All;
    bool allowed_left = isLeft(table_join->kind()) && (table_join->strictness() == JoinStrictness::Any ||
                                                          table_join->strictness() == JoinStrictness::All ||
                                                          table_join->strictness() == JoinStrictness::Semi ||
                                                          table_join->strictness() == JoinStrictness::Anti);
    if (!allowed_inner && !allowed_left)
        return {};

    const auto & clauses = table_join->getClauses();
    bool only_one_key = clauses.size() == 1 &&
        clauses[0].key_names_left.size() == 1 &&
        clauses[0].key_names_right.size() == 1 &&
        !clauses[0].on_filter_condition_left &&
        !clauses[0].on_filter_condition_right &&
        clauses[0].analyzer_left_filter_condition_column_name.empty() &&
        clauses[0].analyzer_right_filter_condition_column_name.empty();

    if (!only_one_key)
        return {};

    const String & key_name = clauses[0].key_names_right[0];

    auto & right_table_expression_data = planner_context->getTableExpressionDataOrThrow(right_table_expression);

    if (const auto * table_column_name = right_table_expression_data.getColumnNameOrNull(key_name))
    {
        const auto & storage_primary_key = storage->getPrimaryKey();
        if (storage_primary_key.size() != 1 || storage_primary_key[0] != *table_column_name)
            return {};
    }
    else
    {
        return {};
    }

    /** For right table expression during execution columns have unique name.
      * Direct key value join implementation during storage querying must use storage column names.
      *
      * Example:
      * CREATE DICTIONARY test_dictionary (id UInt64, value String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'test_dictionary_table')) LIFETIME(0);
      * SELECT t1.id FROM test_table AS t1 INNER JOIN test_dictionary AS t2 ON t1.id = t2.id;
      *
      * Unique execution name for `id` column from right table expression `test_dictionary AS t2` for example can be `t2.id_0`.
      * Storage column name is `id`.
      *
      * Here we create header for right table expression with original storage column names.
      */
    Block right_table_expression_header_with_storage_column_names;

    for (const auto & right_table_expression_column : right_table_expression_header)
    {
        const auto * table_column_name = right_table_expression_data.getColumnNameOrNull(right_table_expression_column.name);
        if (!table_column_name)
            return {};

        auto right_table_expression_column_with_storage_column_name = right_table_expression_column;
        right_table_expression_column_with_storage_column_name.name = *table_column_name;
        right_table_expression_header_with_storage_column_names.insert(right_table_expression_column_with_storage_column_name);
    }

    return std::make_shared<DirectKeyValueJoin>(table_join, right_table_expression_header, storage, right_table_expression_header_with_storage_column_names);
}
}

static std::shared_ptr<IJoin> tryCreateJoin(JoinAlgorithm algorithm,
    std::shared_ptr<TableJoin> & table_join,
    const QueryTreeNodePtr & right_table_expression,
    const Block & left_table_expression_header,
    const Block & right_table_expression_header,
    const PlannerContextPtr & planner_context)
{
    if (table_join->kind() == JoinKind::Paste)
        return std::make_shared<PasteJoin>(table_join, right_table_expression_header);
    /// Direct JOIN with special storages that support key value access. For example JOIN with Dictionary
    if (algorithm == JoinAlgorithm::DIRECT || algorithm == JoinAlgorithm::DEFAULT)
    {
        JoinPtr direct_join = tryDirectJoin(table_join, right_table_expression, right_table_expression_header, planner_context);
        if (direct_join)
            return direct_join;
    }

    if (algorithm == JoinAlgorithm::PARTIAL_MERGE ||
        algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE)
    {
        if (MergeJoin::isSupported(table_join))
            return std::make_shared<MergeJoin>(table_join, right_table_expression_header);
    }

    if (algorithm == JoinAlgorithm::HASH ||
        /// partial_merge is preferred, but can't be used for specified kind of join, fallback to hash
        algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE ||
        algorithm == JoinAlgorithm::PARALLEL_HASH ||
        algorithm == JoinAlgorithm::DEFAULT)
    {
        auto query_context = planner_context->getQueryContext();
        if (table_join->allowParallelHashJoin())
        {
            const auto & settings = query_context->getSettingsRef();
            StatsCollectingParams params{
                calculateCacheKey(table_join, right_table_expression),
                settings[Setting::collect_hash_table_stats_during_joins],
                query_context->getServerSettings().max_entries_for_hash_table_stats,
                settings[Setting::max_size_to_preallocate_for_joins]};
            return std::make_shared<ConcurrentHashJoin>(
                query_context, table_join, query_context->getSettingsRef()[Setting::max_threads], right_table_expression_header, params);
        }

        return std::make_shared<HashJoin>(
            table_join, right_table_expression_header, query_context->getSettingsRef()[Setting::join_any_take_last_row]);
    }

    if (algorithm == JoinAlgorithm::FULL_SORTING_MERGE)
    {
        if (FullSortingMergeJoin::isSupported(table_join))
            return std::make_shared<FullSortingMergeJoin>(table_join, right_table_expression_header);
    }

    if (algorithm == JoinAlgorithm::GRACE_HASH)
    {
        if (GraceHashJoin::isSupported(table_join))
        {
            auto query_context = planner_context->getQueryContext();
            return std::make_shared<GraceHashJoin>(
                query_context,
                table_join,
                left_table_expression_header,
                right_table_expression_header,
                query_context->getTempDataOnDisk());
        }
    }

    if (algorithm == JoinAlgorithm::AUTO)
    {
        if (MergeJoin::isSupported(table_join))
            return std::make_shared<JoinSwitcher>(table_join, right_table_expression_header);
        return std::make_shared<HashJoin>(table_join, right_table_expression_header);
    }

    return nullptr;
}

std::shared_ptr<IJoin> chooseJoinAlgorithm(std::shared_ptr<TableJoin> & table_join,
    const QueryTreeNodePtr & right_table_expression,
    const Block & left_table_expression_header,
    const Block & right_table_expression_header,
    const PlannerContextPtr & planner_context)
{
    if (table_join->getMixedJoinExpression()
        && !table_join->isEnabledAlgorithm(JoinAlgorithm::HASH)
        && !table_join->isEnabledAlgorithm(JoinAlgorithm::GRACE_HASH))
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "JOIN with mixed conditions supports only hash join or grace hash join");
    }

    trySetStorageInTableJoin(right_table_expression, table_join);

    auto & right_table_expression_data = planner_context->getTableExpressionDataOrThrow(right_table_expression);

    /// JOIN with JOIN engine.
    if (auto storage = table_join->getStorageJoin())
    {
        Names required_column_names;
        for (const auto & result_column : right_table_expression_header)
        {
            const auto * source_column_name = right_table_expression_data.getColumnNameOrNull(result_column.name);
            if (!source_column_name)
                throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
                    "JOIN with 'Join' table engine should be performed by storage keys [{}], but column '{}' was found",
                    fmt::join(storage->getKeyNames(), ", "), result_column.name);

            table_join->setRename(*source_column_name, result_column.name);
            required_column_names.push_back(*source_column_name);
        }
        return storage->getJoinLocked(table_join, planner_context->getQueryContext(), required_column_names);
    }

    /** JOIN with constant.
      * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
      */
    if (table_join->isJoinWithConstant())
    {
        if (!table_join->isEnabledAlgorithm(JoinAlgorithm::HASH))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JOIN ON constant supported only with join algorithm 'hash'");

        return std::make_shared<HashJoin>(table_join, right_table_expression_header);
    }

    /** We have only one way to execute a CROSS JOIN - with a hash join.
      * Therefore, for a query with an explicit CROSS JOIN, it should not fail because of the `join_algorithm` setting.
      * If the user expects CROSS JOIN + WHERE to be rewritten to INNER join and to be executed with a specific algorithm,
      * then the setting `cross_to_inner_join_rewrite` may be used, and unsupported cases will fail earlier.
      */
    if (table_join->kind() == JoinKind::Cross)
        return std::make_shared<HashJoin>(table_join, right_table_expression_header);

    if (!table_join->oneDisjunct() && !table_join->isEnabledAlgorithm(JoinAlgorithm::HASH) && !table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `hash` join supports multiple ORs for keys in JOIN ON section");

    for (auto algorithm : table_join->getEnabledJoinAlgorithms())
    {
        auto join = tryCreateJoin(algorithm, table_join, right_table_expression, left_table_expression_header, right_table_expression_header, planner_context);
        if (join)
            return join;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Can't execute any of specified algorithms for specified strictness/kind and right storage type");
}

}
