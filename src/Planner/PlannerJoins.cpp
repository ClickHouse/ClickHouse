#include <Planner/PlannerJoins.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeString.h>

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
#include <Interpreters/HashJoin.h>
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

namespace DB
{

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

std::optional<JoinTableSide> extractJoinTableSideFromExpression(//const ActionsDAG::Node * expression_root_node,
    const IQueryTreeNode * expression_root_node,
    //const std::unordered_set<const ActionsDAG::Node *> & join_expression_dag_input_nodes,
    const TableExpressionSet & left_table_expressions,
    const TableExpressionSet & right_table_expressions,
    const JoinNode & join_node)
{
    std::optional<JoinTableSide> table_side;
    std::vector<const IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(expression_root_node);

    // std::cerr << "==== extractJoinTableSideFromExpression\n";
    // std::cerr << "inp nodes" << std::endl;
    // for (const auto * node : join_expression_dag_input_nodes)
    //     std::cerr << reinterpret_cast<const void *>(node) << ' ' << node->result_name << std::endl;


    // std::cerr << "l names" << std::endl;
    // for (const auto & l : left_table_expression_columns_names)
    //     std::cerr << l << std::endl;

    // std::cerr << "r names" << std::endl;
    // for (const auto & r : right_table_expression_columns_names)
    //     std::cerr << r << std::endl;

    // const auto * left_table_expr = join_node.getLeftTableExpression().get();
    // const auto * right_table_expr = join_node.getRightTableExpression().get();

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();

        //std::cerr << "... " << reinterpret_cast<const void *>(node_to_process) << ' ' << node_to_process->result_name << std::endl;

        if (const auto * function_node = node_to_process->as<FunctionNode>())
        {
            for (const auto & child : function_node->getArguments())
                nodes_to_process.push_back(child.get());

            continue;
        }

        const auto * column_node = node_to_process->as<ColumnNode>();
        if (!column_node)
            continue;

        // if (!join_expression_dag_input_nodes.contains(node_to_process))
        //     continue;

        const auto & input_name = column_node->getColumnName();

        // bool left_table_expression_contains_input = left_table_expression_columns_names.contains(input_name);
        // bool right_table_expression_contains_input = right_table_expression_columns_names.contains(input_name);

        // if (!left_table_expression_contains_input && !right_table_expression_contains_input)
        //     throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
        //         "JOIN {} actions has column {} that do not exist in left {} or right {} table expression columns",
        //         join_node.formatASTForErrorMessage(),
        //         input_name,
        //         boost::join(left_table_expression_columns_names, ", "),
        //         boost::join(right_table_expression_columns_names, ", "));

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
        if (table_side && (*table_side) != input_table_side)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression contains column from left and right table",
                join_node.formatASTForErrorMessage());

        table_side = input_table_side;
    }

    return table_side;
}

const ActionsDAG::Node * appendExpression(
    ActionsDAGPtr & dag,
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
    ActionsDAGPtr & left_dag,
    ActionsDAGPtr & right_dag,
    const PlannerContextPtr & planner_context,
    //ActionsDAGPtr join_expression_dag,
    //const std::unordered_set<const ActionsDAG::Node *> & join_expression_dag_input_nodes,
    //const ActionsDAG::Node * join_expressions_actions_node,
    const QueryTreeNodePtr & join_expression,
    const TableExpressionSet & left_table_expressions,
    const TableExpressionSet & right_table_expressions,
    const JoinNode & join_node,
    JoinClause & join_clause)
{
    std::string function_name;

    //std::cerr << join_expression_dag->dumpDAG() << std::endl;
    auto * function_node = join_expression->as<FunctionNode>();
    if (function_node)
        function_name = function_node->getFunction()->getName();

    // if (join_expressions_actions_node->function)
    //     function_name = join_expressions_actions_node->function->getName();

    /// For 'and' function go into children
    if (function_name == "and")
    {
        for (const auto & child : function_node->getArguments())
        {
            buildJoinClause(//join_expression_dag,
                //join_expression_dag_input_nodes,
                left_dag,
                right_dag,
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
        const auto left_child = function_node->getArguments().getNodes().at(0);//join_expressions_actions_node->children.at(0);
        const auto right_child = function_node->getArguments().getNodes().at(1); //join_expressions_actions_node->children.at(1);

        auto left_expression_side_optional = extractJoinTableSideFromExpression(left_child.get(),
            //join_expression_dag_input_nodes,
            left_table_expressions,
            right_table_expressions,
            join_node);

        auto right_expression_side_optional = extractJoinTableSideFromExpression(right_child.get(),
            //join_expression_dag_input_nodes,
            left_table_expressions,
            right_table_expressions,
            join_node);

        if (!left_expression_side_optional && !right_expression_side_optional)
        {
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} ON expression with constants is not supported",
                join_node.formatASTForErrorMessage());
        }
        else if (left_expression_side_optional && !right_expression_side_optional)
        {
            auto & dag = *left_expression_side_optional == JoinTableSide::Left ? left_dag : right_dag;
            const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
            join_clause.addCondition(*left_expression_side_optional, node);
        }
        else if (!left_expression_side_optional && right_expression_side_optional)
        {
            auto & dag = *right_expression_side_optional == JoinTableSide::Left ? left_dag : right_dag;
            const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
            join_clause.addCondition(*right_expression_side_optional, node);
        }
        else
        {
            // std::cerr << "===============\n";
            auto left_expression_side = *left_expression_side_optional;
            auto right_expression_side = *right_expression_side_optional;

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

        return;
    }

    auto expression_side_optional = extractJoinTableSideFromExpression(//join_expressions_actions_node,
        //join_expression_dag_input_nodes,
        join_expression.get(),
        left_table_expressions,
        right_table_expressions,
        join_node);

    if (!expression_side_optional)
        expression_side_optional = JoinTableSide::Right;

    auto expression_side = *expression_side_optional;
    auto & dag = expression_side == JoinTableSide::Left ? left_dag : right_dag;
    const auto * node = appendExpression(dag, join_expression, planner_context, join_node);
    join_clause.addCondition(expression_side, node);
}

JoinClausesAndActions buildJoinClausesAndActions(//const ColumnsWithTypeAndName & join_expression_input_columns,
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    //ActionsDAGPtr join_expression_actions = std::make_shared<ActionsDAG>(join_expression_input_columns);

    ActionsDAGPtr left_join_actions = std::make_shared<ActionsDAG>(left_table_expression_columns);
    ActionsDAGPtr right_join_actions = std::make_shared<ActionsDAG>(right_table_expression_columns);

    // LOG_TRACE(getLogger("Planner"), "buildJoinClausesAndActions cols {} ", left_join_actions->dumpDAG());
    // LOG_TRACE(getLogger("Planner"), "buildJoinClausesAndActions cols {} ", right_join_actions->dumpDAG());

    /** In ActionsDAG if input node has constant representation additional constant column is added.
      * That way we cannot simply check that node has INPUT type during resolution of expression join table side.
      * Put all nodes after actions dag initialization in set.
      * To check if actions dag node is input column, we check if set contains it.
      */
    // const auto & join_expression_actions_nodes = join_expression_actions->getNodes();

    // std::unordered_set<const ActionsDAG::Node *> join_expression_dag_input_nodes;
    // join_expression_dag_input_nodes.reserve(join_expression_actions_nodes.size());
    // for (const auto & node : join_expression_actions_nodes)
    //     join_expression_dag_input_nodes.insert(&node);

    /** It is possible to have constant value in JOIN ON section, that we need to ignore during DAG construction.
      * If we do not ignore it, this function will be replaced by underlying constant.
      * For example ASOF JOIN does not support JOIN with constants, and we should process it like ordinary JOIN.
      *
      * Example: SELECT * FROM (SELECT 1 AS id, 1 AS value) AS t1 ASOF LEFT JOIN (SELECT 1 AS id, 1 AS value) AS t2
      * ON (t1.id = t2.id) AND 1 != 1 AND (t1.value >= t1.value);
      */
    auto join_expression = join_node.getJoinExpression();
    // LOG_TRACE(getLogger("Planner"), "buildJoinClausesAndActions expr {} ", join_expression->formatConvertedASTForErrorMessage());
    // LOG_TRACE(getLogger("Planner"), "buildJoinClausesAndActions expr {} ", join_expression->dumpTree());

    auto * constant_join_expression = join_expression->as<ConstantNode>();

    if (constant_join_expression && constant_join_expression->hasSourceExpression())
        join_expression = constant_join_expression->getSourceExpression();

    auto * function_node = join_expression->as<FunctionNode>();
    if (!function_node)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            join_node.formatASTForErrorMessage());

    // PlannerActionsVisitor join_expression_visitor(planner_context);
    // auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(join_expression_actions, join_expression);
    // if (join_expression_dag_node_raw_pointers.size() != 1)
    //     throw Exception(ErrorCodes::LOGICAL_ERROR,
    //         "JOIN {} ON clause contains multiple expressions",
    //         join_node.formatASTForErrorMessage());

    // const auto * join_expressions_actions_root_node = join_expression_dag_node_raw_pointers[0];
    // if (!join_expressions_actions_root_node->function)
    //     throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
    //         "JOIN {} join expression expected function",
    //         join_node.formatASTForErrorMessage());

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
    //result.join_expression_actions = join_expression_actions;

    const auto & function_name = function_node->getFunction()->getName();
    if (function_name == "or")
    {
        for (const auto & child : function_node->getArguments())
        {
            result.join_clauses.emplace_back();

            buildJoinClause(//join_expression_actions,
                //join_expression_dag_input_nodes,
                left_join_actions,
                right_join_actions,
                planner_context,
                child,
                join_left_table_expressions,
                join_right_table_expressions,
                join_node,
                result.join_clauses.back());
        }
    }
    else
    {
        result.join_clauses.emplace_back();

        buildJoinClause(
                left_join_actions,
                right_join_actions,
                planner_context,
                //join_expression_actions,
                //join_expression_dag_input_nodes,
                join_expression, //join_expressions_actions_root_node,
                join_left_table_expressions,
                join_right_table_expressions,
                join_node,
                result.join_clauses.back());
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

    for (auto & join_clause : result.join_clauses)
    {
        const auto & left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
        if (!left_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (left_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &left_join_actions->addFunction(and_function, left_filter_condition_nodes, {});
            else
                dag_filter_condition_node = left_filter_condition_nodes[0];

            join_clause.getLeftFilterConditionNodes() = {dag_filter_condition_node};
            left_join_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Left, dag_filter_condition_node->result_name);
        }

        const auto & right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
        if (!right_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (right_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &right_join_actions->addFunction(and_function, right_filter_condition_nodes, {});
            else
                dag_filter_condition_node = right_filter_condition_nodes[0];

            join_clause.getRightFilterConditionNodes() = {dag_filter_condition_node};
            right_join_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Right, dag_filter_condition_node->result_name);
        }

        assert(join_clause.getLeftKeyNodes().size() == join_clause.getRightKeyNodes().size());
        size_t join_clause_key_nodes_size = join_clause.getLeftKeyNodes().size();

        if (join_clause_key_nodes_size == 0)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "JOIN {} cannot get JOIN keys",
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
                    left_key_node = &left_join_actions->addCast(*left_key_node, common_type, {});

                if (!right_key_node->result_type->equals(*common_type))
                    right_key_node = &right_join_actions->addCast(*right_key_node, common_type, {});
            }

            if (join_clause.isNullsafeCompareKey(i) && left_key_node->result_type->isNullable() && right_key_node->result_type->isNullable())
            {
                /**
                  * In case of null-safe comparison (a IS NOT DISTICT FROM b),
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
                left_key_node = &left_join_actions->addFunction(wrap_nullsafe_function, {left_key_node}, {});
                right_key_node = &right_join_actions->addFunction(wrap_nullsafe_function, {right_key_node}, {});
            }

            left_join_actions->addOrReplaceInOutputs(*left_key_node);
            right_join_actions->addOrReplaceInOutputs(*right_key_node);

            add_necessary_name_if_needed(JoinTableSide::Left, left_key_node->result_name);
            add_necessary_name_if_needed(JoinTableSide::Right, right_key_node->result_name);
        }
    }

    result.left_join_expressions_actions = left_join_actions->clone();
    result.left_join_tmp_expression_actions = std::move(left_join_actions);
    result.left_join_expressions_actions->removeUnusedActions(join_left_actions_names);

    // for (const auto & name : join_right_actions_names)
    //     std::cerr << ".. " << name << std::endl;

    // std::cerr << right_join_actions->dumpDAG() << std::endl;

    result.right_join_expressions_actions = right_join_actions->clone();
    result.right_join_tmp_expression_actions = std::move(right_join_actions);
    result.right_join_expressions_actions->removeUnusedActions(join_right_actions_names);

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

    // auto join_expression_input_columns = left_table_expression_columns;
    // join_expression_input_columns.insert(join_expression_input_columns.end(), right_table_expression_columns.begin(), right_table_expression_columns.end());

    return buildJoinClausesAndActions(/*join_expression_input_columns,*/ left_table_expression_columns, right_table_expression_columns, join_node_typed, planner_context);
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
        if (table_join->allowParallelHashJoin())
        {
            auto query_context = planner_context->getQueryContext();
            return std::make_shared<ConcurrentHashJoin>(query_context, table_join, query_context->getSettings().max_threads, right_table_expression_header);
        }

        return std::make_shared<HashJoin>(table_join, right_table_expression_header);
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
