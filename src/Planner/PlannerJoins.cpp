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
#include <Functions/FunctionsConversion.h>
#include <Functions/CastOverloadResolver.h>

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

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;

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
        buffer << " left_condition_nodes: " + dump_dag_nodes(right_filter_condition_nodes);
}

String JoinClause::dump() const
{
    WriteBufferFromOwnString buffer;
    dump(buffer);

    return buffer.str();
}

namespace
{

std::optional<JoinTableSide> extractJoinTableSideFromExpression(const ActionsDAG::Node * expression_root_node,
    const std::unordered_set<const ActionsDAG::Node *> & join_expression_dag_input_nodes,
    const NameSet & left_table_expression_columns_names,
    const NameSet & right_table_expression_columns_names,
    const JoinNode & join_node)
{
    std::optional<JoinTableSide> table_side;
    std::vector<const ActionsDAG::Node *> nodes_to_process;
    nodes_to_process.push_back(expression_root_node);

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();

        for (const auto & child : node_to_process->children)
            nodes_to_process.push_back(child);

        if (!join_expression_dag_input_nodes.contains(node_to_process))
            continue;

        const auto & input_name = node_to_process->result_name;

        bool left_table_expression_contains_input = left_table_expression_columns_names.contains(input_name);
        bool right_table_expression_contains_input = right_table_expression_columns_names.contains(input_name);

        if (!left_table_expression_contains_input && !right_table_expression_contains_input)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} actions has column {} that do not exist in left {} or right {} table expression columns",
                join_node.formatASTForErrorMessage(),
                input_name,
                boost::join(left_table_expression_columns_names, ", "),
                boost::join(right_table_expression_columns_names, ", "));

        auto input_table_side = left_table_expression_contains_input ? JoinTableSide::Left : JoinTableSide::Right;
        if (table_side && (*table_side) != input_table_side)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression contains column from left and right table",
                join_node.formatASTForErrorMessage());

        table_side = input_table_side;
    }

    return table_side;
}

void buildJoinClause(ActionsDAGPtr join_expression_dag,
    const std::unordered_set<const ActionsDAG::Node *> & join_expression_dag_input_nodes,
    const ActionsDAG::Node * join_expressions_actions_node,
    const NameSet & left_table_expression_columns_names,
    const NameSet & right_table_expression_columns_names,
    const JoinNode & join_node,
    JoinClause & join_clause)
{
    std::string function_name;

    if (join_expressions_actions_node->function)
        function_name = join_expressions_actions_node->function->getName();

    /// For 'and' function go into children
    if (function_name == "and")
    {
        for (const auto & child : join_expressions_actions_node->children)
        {
            buildJoinClause(join_expression_dag,
                join_expression_dag_input_nodes,
                child,
                left_table_expression_columns_names,
                right_table_expression_columns_names,
                join_node,
                join_clause);
        }

        return;
    }

    auto asof_inequality = getASOFJoinInequality(function_name);
    bool is_asof_join_inequality = join_node.getStrictness() == JoinStrictness::Asof && asof_inequality != ASOFJoinInequality::None;

    if (function_name == "equals" || is_asof_join_inequality)
    {
        const auto * left_child = join_expressions_actions_node->children.at(0);
        const auto * right_child = join_expressions_actions_node->children.at(1);

        auto left_expression_side_optional = extractJoinTableSideFromExpression(left_child,
            join_expression_dag_input_nodes,
            left_table_expression_columns_names,
            right_table_expression_columns_names,
            join_node);

        auto right_expression_side_optional = extractJoinTableSideFromExpression(right_child,
            join_expression_dag_input_nodes,
            left_table_expression_columns_names,
            right_table_expression_columns_names,
            join_node);

        if (!left_expression_side_optional && !right_expression_side_optional)
        {
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} ON expression {} with constants is not supported",
                join_node.formatASTForErrorMessage(),
                join_expressions_actions_node->function->getName());
        }
        else if (left_expression_side_optional && !right_expression_side_optional)
        {
            join_clause.addCondition(*left_expression_side_optional, join_expressions_actions_node);
        }
        else if (!left_expression_side_optional && right_expression_side_optional)
        {
            join_clause.addCondition(*right_expression_side_optional, join_expressions_actions_node);
        }
        else
        {
            auto left_expression_side = *left_expression_side_optional;
            auto right_expression_side = *right_expression_side_optional;

            if (left_expression_side != right_expression_side)
            {
                const ActionsDAG::Node * left_key = left_child;
                const ActionsDAG::Node * right_key = right_child;

                if (left_expression_side_optional == JoinTableSide::Right)
                {
                    left_key = right_child;
                    right_key = left_child;
                    asof_inequality = reverseASOFJoinInequality(asof_inequality);
                }

                if (is_asof_join_inequality)
                {
                    if (join_clause.hasASOF())
                    {
                        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                            "JOIN {} ASOF JOIN expects exactly one inequality in ON section",
                            join_node.formatASTForErrorMessage());
                    }

                    join_clause.addASOFKey(left_key, right_key, asof_inequality);
                }
                else
                {
                    join_clause.addKey(left_key, right_key);
                }
            }
            else
            {
                join_clause.addCondition(left_expression_side, join_expressions_actions_node);
            }
        }

        return;
    }

    auto expression_side_optional = extractJoinTableSideFromExpression(join_expressions_actions_node,
        join_expression_dag_input_nodes,
        left_table_expression_columns_names,
        right_table_expression_columns_names,
        join_node);

    if (!expression_side_optional)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} with constants is not supported",
                join_node.formatASTForErrorMessage());

    auto expression_side = *expression_side_optional;

    join_clause.addCondition(expression_side, join_expressions_actions_node);
}

JoinClausesAndActions buildJoinClausesAndActions(const ColumnsWithTypeAndName & join_expression_input_columns,
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    ActionsDAGPtr join_expression_actions = std::make_shared<ActionsDAG>(join_expression_input_columns);

    /** In ActionsDAG if input node has constant representation additional constant column is added.
      * That way we cannot simply check that node has INPUT type during resolution of expression join table side.
      * Put all nodes after actions dag initialization in set.
      * To check if actions dag node is input column, we set contains node.
      */
    const auto & join_expression_actions_nodes = join_expression_actions->getNodes();

    std::unordered_set<const ActionsDAG::Node *> join_expression_dag_input_nodes;
    join_expression_dag_input_nodes.reserve(join_expression_actions_nodes.size());
    for (const auto & node : join_expression_actions_nodes)
        join_expression_dag_input_nodes.insert(&node);

    PlannerActionsVisitor join_expression_visitor(planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(join_expression_actions, join_node.getJoinExpression());
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JOIN {} ON clause contains multiple expressions",
            join_node.formatASTForErrorMessage());

    const auto * join_expressions_actions_root_node = join_expression_dag_node_raw_pointers[0];
    if (!join_expressions_actions_root_node->function)
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

    JoinClausesAndActions result;
    result.join_expression_actions = join_expression_actions;

    const auto & function_name = join_expressions_actions_root_node->function->getName();
    if (function_name == "or")
    {
        for (const auto & child : join_expressions_actions_root_node->children)
        {
            result.join_clauses.emplace_back();

            buildJoinClause(join_expression_actions,
                join_expression_dag_input_nodes,
                child,
                join_left_actions_names_set,
                join_right_actions_names_set,
                join_node,
                result.join_clauses.back());
        }
    }
    else
    {
        result.join_clauses.emplace_back();

        buildJoinClause(join_expression_actions,
                join_expression_dag_input_nodes,
                join_expressions_actions_root_node,
                join_left_actions_names_set,
                join_right_actions_names_set,
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
                dag_filter_condition_node = &join_expression_actions->addFunction(and_function, left_filter_condition_nodes, {});
            else
                dag_filter_condition_node = left_filter_condition_nodes[0];

            join_clause.getLeftFilterConditionNodes().clear();
            join_clause.addCondition(JoinTableSide::Left, dag_filter_condition_node);

            join_expression_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Left, dag_filter_condition_node->result_name);
        }

        const auto & right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
        if (!right_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (right_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &join_expression_actions->addFunction(and_function, right_filter_condition_nodes, {});
            else
                dag_filter_condition_node = right_filter_condition_nodes[0];

            join_clause.getRightFilterConditionNodes().clear();
            join_clause.addCondition(JoinTableSide::Right, dag_filter_condition_node);

            join_expression_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

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
                }

                auto cast_type_name = common_type->getName();
                Field cast_type_constant_value(cast_type_name);

                ColumnWithTypeAndName cast_column;
                cast_column.name = calculateConstantActionNodeName(cast_type_constant_value);
                cast_column.column = DataTypeString().createColumnConst(0, cast_type_constant_value);
                cast_column.type = std::make_shared<DataTypeString>();

                const ActionsDAG::Node * cast_type_constant_node = nullptr;

                if (!left_key_node->result_type->equals(*common_type))
                {
                    cast_type_constant_node = &join_expression_actions->addColumn(cast_column);

                    FunctionCastBase::Diagnostic diagnostic = {left_key_node->result_name, left_key_node->result_name};
                    FunctionOverloadResolverPtr func_builder_cast
                        = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(diagnostic);

                    ActionsDAG::NodeRawConstPtrs children = {left_key_node, cast_type_constant_node};
                    left_key_node = &join_expression_actions->addFunction(func_builder_cast, std::move(children), {});
                }

                if (!right_key_node->result_type->equals(*common_type))
                {
                    if (!cast_type_constant_node)
                        cast_type_constant_node = &join_expression_actions->addColumn(cast_column);

                    FunctionCastBase::Diagnostic diagnostic = {right_key_node->result_name, right_key_node->result_name};
                    FunctionOverloadResolverPtr func_builder_cast
                        = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

                    ActionsDAG::NodeRawConstPtrs children = {right_key_node, cast_type_constant_node};
                    right_key_node = &join_expression_actions->addFunction(func_builder_cast, std::move(children), {});
                }
            }

            join_expression_actions->addOrReplaceInOutputs(*left_key_node);
            join_expression_actions->addOrReplaceInOutputs(*right_key_node);

            add_necessary_name_if_needed(JoinTableSide::Left, left_key_node->result_name);
            add_necessary_name_if_needed(JoinTableSide::Right, right_key_node->result_name);
        }
    }

    result.left_join_expressions_actions = join_expression_actions->clone();
    result.left_join_expressions_actions->removeUnusedActions(join_left_actions_names);

    result.right_join_expressions_actions = join_expression_actions->clone();
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

    auto join_expression_input_columns = left_table_expression_columns;
    join_expression_input_columns.insert(join_expression_input_columns.end(), right_table_expression_columns.begin(), right_table_expression_columns.end());

    return buildJoinClausesAndActions(join_expression_input_columns, left_table_expression_columns, right_table_expression_columns, join_node_typed, planner_context);
}

std::optional<bool> tryExtractConstantFromJoinNode(const QueryTreeNodePtr & join_node)
{
    auto & join_node_typed = join_node->as<JoinNode &>();
    if (!join_node_typed.getJoinExpression())
        return {};

    auto constant_value = join_node_typed.getJoinExpression()->getConstantValueOrNull();
    if (!constant_value)
        return {};

    const auto & value = constant_value->getValue();
    auto constant_type = constant_value->getType();
    constant_type = removeNullable(removeLowCardinality(constant_type));

    auto which_constant_type = WhichDataType(constant_type);
    if (!which_constant_type.isUInt8() && !which_constant_type.isNothing())
        return {};

    if (value.isNull())
        return false;

    UInt8 predicate_value = value.safeGet<UInt8>();
    return predicate_value > 0;
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

    if (auto storage_join = std::dynamic_pointer_cast<StorageJoin>(storage); storage_join)
    {
        table_join->setStorageJoin(storage_join);
        return;
    }

    if (!table_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT))
        return;

    if (auto storage_dictionary = std::dynamic_pointer_cast<StorageDictionary>(storage); storage_dictionary)
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
        !clauses[0].on_filter_condition_right;

    if (!only_one_key)
        return {};

    const String & key_name = clauses[0].key_names_right[0];

    auto & right_table_expression_data = planner_context->getTableExpressionDataOrThrow(right_table_expression);
    const auto * table_column_name = right_table_expression_data.getColumnNameOrNull(key_name);
    if (!table_column_name)
        return {};

    const auto & storage_primary_key = storage->getPrimaryKey();
    if (storage_primary_key.size() != 1 || storage_primary_key[0] != *table_column_name)
        return {};

    /** For right table expression during execution columns have unique name.
      * Direct key value join implementation during storage quering must use storage column names.
      *
      * Example:
      * CREATE DICTIONARY test_dictionary (id UInt64, value String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'test_dictionary_table')) LIFETIME(0);
      * SELECT t1.id FROM test_table AS t1 INNER JOIN test_dictionary AS t2 ON t1.id = t2.id;
      *
      * Unique execution name for `id` column in right table expression `test_dictionary AS t2` for example can be `t2.id_0`.
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

std::shared_ptr<IJoin> chooseJoinAlgorithm(std::shared_ptr<TableJoin> & table_join,
    const QueryTreeNodePtr & right_table_expression,
    const Block & right_table_expression_header,
    const PlannerContextPtr & planner_context)
{
    trySetStorageInTableJoin(right_table_expression, table_join);

    /// JOIN with JOIN engine.
    if (auto storage = table_join->getStorageJoin())
        return storage->getJoinLocked(table_join, planner_context->getQueryContext());

    /** JOIN with constant.
      * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
      */
    if (table_join->isJoinWithConstant())
    {
        if (!table_join->isEnabledAlgorithm(JoinAlgorithm::HASH))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JOIN with constant supported only with join algorithm 'hash'");

        return std::make_shared<HashJoin>(table_join, right_table_expression_header);
    }

    if (!table_join->oneDisjunct() && !table_join->isEnabledAlgorithm(JoinAlgorithm::HASH))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only `hash` join supports multiple ORs for keys in JOIN ON section");

    /// Direct JOIN with special storages that support key value access. For example JOIN with Dictionary.
    if (table_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT))
    {
        JoinPtr direct_join = tryDirectJoin(table_join, right_table_expression, right_table_expression_header, planner_context);
        if (direct_join)
            return direct_join;
    }

    if (table_join->isEnabledAlgorithm(JoinAlgorithm::PARTIAL_MERGE) ||
        table_join->isEnabledAlgorithm(JoinAlgorithm::PREFER_PARTIAL_MERGE))
    {
        if (MergeJoin::isSupported(table_join))
            return std::make_shared<MergeJoin>(table_join, right_table_expression_header);
    }

    if (table_join->isEnabledAlgorithm(JoinAlgorithm::HASH) ||
        /// partial_merge is preferred, but can't be used for specified kind of join, fallback to hash
        table_join->isEnabledAlgorithm(JoinAlgorithm::PREFER_PARTIAL_MERGE) ||
        table_join->isEnabledAlgorithm(JoinAlgorithm::PARALLEL_HASH))
    {
        if (table_join->allowParallelHashJoin())
        {
            auto query_context = planner_context->getQueryContext();
            return std::make_shared<ConcurrentHashJoin>(query_context, table_join, query_context->getSettings().max_threads, right_table_expression_header);
        }

        return std::make_shared<HashJoin>(table_join, right_table_expression_header);
    }

    if (table_join->isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE))
    {
        if (FullSortingMergeJoin::isSupported(table_join))
            return std::make_shared<FullSortingMergeJoin>(table_join, right_table_expression_header);
    }

    if (table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO))
        return std::make_shared<JoinSwitcher>(table_join, right_table_expression_header);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't execute any of specified algorithms for specified strictness/kind and right storage type");
}

}
