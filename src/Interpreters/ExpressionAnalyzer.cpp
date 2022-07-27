#include <memory>
#include <Core/Block.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTInterpolateElement.h>

#include <DataTypes/DataTypeNullable.h>
#include <Columns/IColumn.h>

#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/DictionaryReader.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/DirectJoin.h>
#include <Interpreters/Set.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>

#include <Processors/QueryPlan/ExpressionStep.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>


#include <Dictionaries/DictionaryStructure.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnNullable.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Core/SettingsEnums.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Common/logger_useful.h>


#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/join_common.h>
#include <Interpreters/misc.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Parsers/formatAST.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_PREWHERE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
}

namespace
{

/// Check if there is an ignore function. It's used for disabling constant folding in query
///  predicates because some performance tests use ignore function as a non-optimize guard.
bool allowEarlyConstantFolding(const ActionsDAG & actions, const Settings & settings)
{
    if (!settings.enable_early_constant_folding)
        return false;

    for (const auto & node : actions.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
        {
            if (!node.function_base->isSuitableForConstantFolding())
                return false;
        }
    }
    return true;
}

bool checkPositionalArguments(ASTPtr & argument, const ASTSelectQuery * select_query, ASTSelectQuery::Expression expression)
{
    auto columns = select_query->select()->children;

    const auto * expr_with_alias = dynamic_cast<const ASTWithAlias *>(argument.get());
    if (expr_with_alias && !expr_with_alias->alias.empty())
        return false;

    const auto * ast_literal = typeid_cast<const ASTLiteral *>(argument.get());
    if (!ast_literal)
        return false;

    auto which = ast_literal->value.getType();
    if (which != Field::Types::UInt64)
        return false;

    auto pos = ast_literal->value.get<UInt64>();
    if (!pos || pos > columns.size())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Positional argument out of bounds: {} (exprected in range [1, {}]",
                        pos, columns.size());

    const auto & column = columns[--pos];
    if (typeid_cast<const ASTIdentifier *>(column.get()) || typeid_cast<const ASTLiteral *>(column.get()))
    {
        argument = column->clone();
    }
    else if (typeid_cast<const ASTFunction *>(column.get()))
    {
        std::function<void(ASTPtr)> throw_if_aggregate_function = [&](ASTPtr node)
        {
            if (const auto * function = typeid_cast<const ASTFunction *>(node.get()))
            {
                auto is_aggregate_function = AggregateUtils::isAggregateFunction(*function);
                if (is_aggregate_function)
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal value (aggregate function) for positional argument in {}",
                                    ASTSelectQuery::expressionToString(expression));
                }
                else
                {
                    if (function->arguments)
                    {
                        for (const auto & arg : function->arguments->children)
                            throw_if_aggregate_function(arg);
                    }
                }
            }
        };

        if (expression == ASTSelectQuery::Expression::GROUP_BY)
            throw_if_aggregate_function(column);

        argument = column->clone();
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal value for positional argument in {}",
                        ASTSelectQuery::expressionToString(expression));
    }

    return true;
}

void replaceForPositionalArguments(ASTPtr & argument, const ASTSelectQuery * select_query, ASTSelectQuery::Expression expression)
{
    auto argument_with_replacement = argument->clone();
    if (checkPositionalArguments(argument_with_replacement, select_query, expression))
        argument = argument_with_replacement;
}

}

bool sanitizeBlock(Block & block, bool throw_if_cannot_create_column)
{
    for (auto & col : block)
    {
        if (!col.column)
        {
            if (isNotCreatable(col.type->getTypeId()))
            {
                if (throw_if_cannot_create_column)
                    throw Exception("Cannot create column of type " + col.type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                return false;
            }

            col.column = col.type->createColumn();
        }
        else if (!col.column->empty())
            col.column = col.column->cloneEmpty();
    }
    return true;
}

ExpressionAnalyzerData::~ExpressionAnalyzerData() = default;

ExpressionAnalyzer::ExtractedSettings::ExtractedSettings(const Settings & settings_)
    : use_index_for_in_with_subqueries(settings_.use_index_for_in_with_subqueries)
    , size_limits_for_set(settings_.max_rows_in_set, settings_.max_bytes_in_set, settings_.set_overflow_mode)
    , distributed_group_by_no_merge(settings_.distributed_group_by_no_merge)
{}

ExpressionAnalyzer::~ExpressionAnalyzer() = default;

ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const TreeRewriterResultPtr & syntax_analyzer_result_,
    ContextPtr context_,
    size_t subquery_depth_,
    bool do_global,
    bool is_explain,
    PreparedSetsPtr prepared_sets_)
    : WithContext(context_)
    , query(query_), settings(getContext()->getSettings())
    , subquery_depth(subquery_depth_)
    , syntax(syntax_analyzer_result_)
{
    /// Cache prepared sets because we might run analysis multiple times
    if (prepared_sets_)
        prepared_sets = prepared_sets_;
    else
        prepared_sets = std::make_shared<PreparedSets>();

    /// external_tables, sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables(do_global, is_explain);

    auto temp_actions = std::make_shared<ActionsDAG>(sourceColumns());
    columns_after_array_join = getColumnsAfterArrayJoin(temp_actions, sourceColumns());
    columns_after_join = analyzeJoin(temp_actions, columns_after_array_join);
    /// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
    /// This analysis should be performed after processing global subqueries, because otherwise,
    /// if the aggregate function contains a global subquery, then `analyzeAggregation` method will save
    /// in `aggregate_descriptions` the information about the parameters of this aggregate function, among which
    /// global subquery. Then, when you call `initGlobalSubqueriesAndExternalTables` method, this
    /// the global subquery will be replaced with a temporary table, resulting in aggregate_descriptions
    /// will contain out-of-date information, which will lead to an error when the query is executed.
    analyzeAggregation(temp_actions);
}

NamesAndTypesList ExpressionAnalyzer::getColumnsAfterArrayJoin(ActionsDAGPtr & actions, const NamesAndTypesList & src_columns)
{
    const auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        return {};

    auto [array_join_expression_list, is_array_join_left] = select_query->arrayJoinExpressionList();

    if (!array_join_expression_list)
        return src_columns;

    getRootActionsNoMakeSet(array_join_expression_list, actions, false);

    auto array_join = addMultipleArrayJoinAction(actions, is_array_join_left);
    auto sample_columns = actions->getResultColumns();
    array_join->prepare(sample_columns);
    actions = std::make_shared<ActionsDAG>(sample_columns);

    NamesAndTypesList new_columns_after_array_join;
    NameSet added_columns;

    for (auto & column : actions->getResultColumns())
    {
        if (syntax->array_join_result_to_source.contains(column.name))
        {
            new_columns_after_array_join.emplace_back(column.name, column.type);
            added_columns.emplace(column.name);
        }
    }

    for (const auto & column : src_columns)
        if (!added_columns.contains(column.name))
            new_columns_after_array_join.emplace_back(column.name, column.type);

    return new_columns_after_array_join;
}

NamesAndTypesList ExpressionAnalyzer::analyzeJoin(ActionsDAGPtr & actions, const NamesAndTypesList & src_columns)
{
    const auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        return {};

    const ASTTablesInSelectQueryElement * join = select_query->join();
    if (join)
    {
        getRootActionsNoMakeSet(analyzedJoin().leftKeysList(), actions, false);
        auto sample_columns = actions->getNamesAndTypesList();
        syntax->analyzed_join->addJoinedColumnsAndCorrectTypes(sample_columns, true);
        actions = std::make_shared<ActionsDAG>(sample_columns);
    }

    NamesAndTypesList result_columns = src_columns;
    syntax->analyzed_join->addJoinedColumnsAndCorrectTypes(result_columns, false);
    return result_columns;
}

void ExpressionAnalyzer::analyzeAggregation(ActionsDAGPtr & temp_actions)
{
    /** Find aggregation keys (aggregation_keys), information about aggregate functions (aggregate_descriptions),
     *  as well as a set of columns obtained after the aggregation, if any,
     *  or after all the actions that are usually performed before aggregation (aggregated_columns).
     *
     * Everything below (compiling temporary ExpressionActions) - only for the purpose of query analysis (type output).
     */

    auto * select_query = query->as<ASTSelectQuery>();

    makeAggregateDescriptions(temp_actions, aggregate_descriptions);
    has_aggregation = !aggregate_descriptions.empty() || (select_query && (select_query->groupBy() || select_query->having()));

    if (!has_aggregation)
    {
        aggregated_columns = temp_actions->getNamesAndTypesList();
        return;
    }

    /// Find out aggregation keys.
    if (select_query)
    {
        if (ASTPtr group_by_ast = select_query->groupBy())
        {
            NameToIndexMap unique_keys;
            ASTs & group_asts = group_by_ast->children;

            if (select_query->group_by_with_rollup)
                group_by_kind = GroupByKind::ROLLUP;
            else if (select_query->group_by_with_cube)
                group_by_kind = GroupByKind::CUBE;
            else if (select_query->group_by_with_grouping_sets && group_asts.size() > 1)
                group_by_kind = GroupByKind::GROUPING_SETS;
            else
                group_by_kind = GroupByKind::ORDINARY;
            bool use_nulls = group_by_kind != GroupByKind::ORDINARY && getContext()->getSettingsRef().group_by_use_nulls;

            /// For GROUPING SETS with multiple groups we always add virtual __grouping_set column
            /// With set number, which is used as an additional key at the stage of merging aggregating data.
            if (group_by_kind != GroupByKind::ORDINARY)
                aggregated_columns.emplace_back("__grouping_set", std::make_shared<DataTypeUInt64>());

            for (ssize_t i = 0; i < static_cast<ssize_t>(group_asts.size()); ++i)
            {
                ssize_t size = group_asts.size();

                if (getContext()->getSettingsRef().enable_positional_arguments)
                    replaceForPositionalArguments(group_asts[i], select_query, ASTSelectQuery::Expression::GROUP_BY);

                if (select_query->group_by_with_grouping_sets)
                {
                    ASTs group_elements_ast;
                    const ASTExpressionList * group_ast_element = group_asts[i]->as<const ASTExpressionList>();
                    group_elements_ast = group_ast_element->children;

                    NamesAndTypesList grouping_set_list;
                    ColumnNumbers grouping_set_indexes_list;

                    for (ssize_t j = 0; j < ssize_t(group_elements_ast.size()); ++j)
                    {
                        getRootActionsNoMakeSet(group_elements_ast[j], temp_actions, false);

                        ssize_t group_size = group_elements_ast.size();
                        const auto & column_name = group_elements_ast[j]->getColumnName();
                        const auto * node = temp_actions->tryFindInIndex(column_name);
                        if (!node)
                            throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                        /// Only removes constant keys if it's an initiator or distributed_group_by_no_merge is enabled.
                        if (getContext()->getClientInfo().distributed_depth == 0 || settings.distributed_group_by_no_merge > 0)
                        {
                            /// Constant expressions have non-null column pointer at this stage.
                            if (node->column && isColumnConst(*node->column))
                            {
                                select_query->group_by_with_constant_keys = true;

                                /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                                if (!aggregate_descriptions.empty() || group_size > 1)
                                {
                                    if (j + 1 < static_cast<ssize_t>(group_size))
                                        group_elements_ast[j] = std::move(group_elements_ast.back());

                                    group_elements_ast.pop_back();

                                    --j;
                                    continue;
                                }
                            }
                        }

                        NameAndTypePair key{column_name, use_nulls ? makeNullableSafe(node->result_type) : node->result_type };

                        grouping_set_list.push_back(key);

                        /// Aggregation keys are unique.
                        if (!unique_keys.contains(key.name))
                        {
                            unique_keys[key.name] = aggregation_keys.size();
                            grouping_set_indexes_list.push_back(aggregation_keys.size());
                            aggregation_keys.push_back(key);

                            /// Key is no longer needed, therefore we can save a little by moving it.
                            aggregated_columns.push_back(std::move(key));
                        }
                        else
                        {
                            grouping_set_indexes_list.push_back(unique_keys[key.name]);
                        }
                    }

                    aggregation_keys_list.push_back(std::move(grouping_set_list));
                    aggregation_keys_indexes_list.push_back(std::move(grouping_set_indexes_list));
                }
                else
                {
                    getRootActionsNoMakeSet(group_asts[i], temp_actions, false);

                    const auto & column_name = group_asts[i]->getColumnName();
                    const auto * node = temp_actions->tryFindInIndex(column_name);
                    if (!node)
                        throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                    /// Only removes constant keys if it's an initiator or distributed_group_by_no_merge is enabled.
                    if (getContext()->getClientInfo().distributed_depth == 0 || settings.distributed_group_by_no_merge > 0)
                    {
                        /// Constant expressions have non-null column pointer at this stage.
                        if (node->column && isColumnConst(*node->column))
                        {
                            select_query->group_by_with_constant_keys = true;

                            /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                            if (!aggregate_descriptions.empty() || size > 1)
                            {
                                if (i + 1 < static_cast<ssize_t>(size))
                                    group_asts[i] = std::move(group_asts.back());

                                group_asts.pop_back();

                                --i;
                                continue;
                            }
                        }
                    }

                    NameAndTypePair key = NameAndTypePair{ column_name, use_nulls ? makeNullableSafe(node->result_type) : node->result_type };

                    /// Aggregation keys are uniqued.
                    if (!unique_keys.contains(key.name))
                    {
                        unique_keys[key.name] = aggregation_keys.size();
                        aggregation_keys.push_back(key);

                        /// Key is no longer needed, therefore we can save a little by moving it.
                        aggregated_columns.push_back(std::move(key));
                    }
                }
            }

            if (!select_query->group_by_with_grouping_sets)
            {
                auto & list = aggregation_keys_indexes_list.emplace_back();
                for (size_t i = 0; i < aggregation_keys.size(); ++i)
                    list.push_back(i);
            }

            if (group_asts.empty())
            {
                select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
                has_aggregation = select_query->having() || !aggregate_descriptions.empty();
            }
        }

        /// Constant expressions are already removed during first 'analyze' run.
        /// So for second `analyze` information is taken from select_query.
        has_const_aggregation_keys = select_query->group_by_with_constant_keys;
    }
    else
        aggregated_columns = temp_actions->getNamesAndTypesList();

    for (const auto & desc : aggregate_descriptions)
        aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables(bool do_global, bool is_explain)
{
    if (do_global)
    {
        GlobalSubqueriesVisitor::Data subqueries_data(
            getContext(), subquery_depth, isRemoteStorage(), is_explain, external_tables, prepared_sets, has_global_subqueries);
        GlobalSubqueriesVisitor(subqueries_data).visit(query);
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name, const SelectQueryOptions & query_options)
{
    if (!prepared_sets)
        return;

    auto set_key = PreparedSetKey::forSubquery(*subquery_or_table_name);

    if (prepared_sets->get(set_key))
        return; /// Already prepared.

    if (auto set_ptr_from_storage_set = isPlainStorageSetInSubquery(subquery_or_table_name))
    {
        prepared_sets->set(set_key, set_ptr_from_storage_set);
        return;
    }

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, getContext(), {}, query_options);
    auto io = interpreter_subquery->execute();
    PullingAsyncPipelineExecutor executor(io.pipeline);

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true, getContext()->getSettingsRef().transform_null_in);
    set->setHeader(executor.getHeader().getColumnsWithTypeAndName());

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block.getColumnsWithTypeAndName()))
            return;
    }

    set->finishInsert();

    prepared_sets->set(set_key, std::move(set));
}

SetPtr ExpressionAnalyzer::isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name)
{
    const auto * table = subquery_or_table_name->as<ASTTableIdentifier>();
    if (!table)
        return nullptr;
    auto table_id = getContext()->resolveStorageID(subquery_or_table_name);
    const auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (storage->getName() != "Set")
        return nullptr;
    const auto storage_set = std::dynamic_pointer_cast<StorageSet>(storage);
    return storage_set->getSet();
}


/// Performance optimization for IN() if storage supports it.
void SelectQueryExpressionAnalyzer::makeSetsForIndex(const ASTPtr & node)
{
    if (!node || !storage() || !storage()->supportsIndexForIn())
        return;

    for (auto & child : node->children)
    {
        /// Don't descend into subqueries.
        if (child->as<ASTSubquery>())
            continue;

        /// Don't descend into lambda functions
        const auto * func = child->as<ASTFunction>();
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndex(child);
    }

    const auto * func = node->as<ASTFunction>();
    if (func && functionIsInOrGlobalInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & left_in_operand = args.children.at(0);

        if (storage()->mayBenefitFromIndexForIn(left_in_operand, getContext(), metadata_snapshot))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTTableIdentifier>())
            {
                if (settings.use_index_for_in_with_subqueries)
                    tryMakeSetForIndexFromSubquery(arg, query_options);
            }
            else
            {
                auto temp_actions = std::make_shared<ActionsDAG>(columns_after_join);
                getRootActions(left_in_operand, true, temp_actions);

                if (prepared_sets && temp_actions->tryFindInIndex(left_in_operand->getColumnName()))
                    makeExplicitSet(func, *temp_actions, true, getContext(), settings.size_limits_for_set, *prepared_sets);
            }
        }
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        only_consts,
        !isRemoteStorage() /* create_source_for_in */,
        getAggregationKeysInfo());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}

void ExpressionAnalyzer::getRootActionsNoMakeSet(const ASTPtr & ast, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        true /* no_makeset_for_subqueries, no_makeset implies no_makeset_for_subqueries */,
        true /* no_makeset */,
        only_consts,
        !isRemoteStorage() /* create_source_for_in */,
        getAggregationKeysInfo());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsForHaving(
    const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        only_consts,
        true /* create_source_for_in */,
        getAggregationKeysInfo());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsForWindowFunctions(const ASTPtr & ast, bool no_makeset_for_subqueries, ActionsDAGPtr & actions)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(
        getContext(),
        settings.size_limits_for_set,
        subquery_depth,
        sourceColumns(),
        std::move(actions),
        prepared_sets,
        no_makeset_for_subqueries,
        false /* no_makeset */,
        false /*only_consts */,
        !isRemoteStorage() /* create_source_for_in */,
        getAggregationKeysInfo(),
        true);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::makeAggregateDescriptions(ActionsDAGPtr & actions, AggregateDescriptions & descriptions)
{
    for (const ASTFunction * node : aggregates())
    {
        AggregateDescription aggregate;
        if (node->arguments)
            getRootActionsNoMakeSet(node->arguments, actions);

        aggregate.column_name = node->getColumnName();

        const ASTs & arguments = node->arguments ? node->arguments->children : ASTs();
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const std::string & name = arguments[i]->getColumnName();
            const auto * dag_node = actions->tryFindInIndex(name);
            if (!dag_node)
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unknown identifier '{}' in aggregate function '{}'",
                    name, node->formatForErrorMessage());
            }

            types[i] = dag_node->result_type;
            aggregate.argument_names[i] = name;
        }

        AggregateFunctionProperties properties;
        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters, "", getContext()) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters, properties);

        descriptions.push_back(aggregate);
    }
}

void ExpressionAnalyzer::makeWindowDescriptionFromAST(const Context & context_,
    const WindowDescriptions & existing_descriptions,
    WindowDescription & desc, const IAST * ast)
{
    const auto & definition = ast->as<const ASTWindowDefinition &>();

    if (!definition.parent_window_name.empty())
    {
        auto it = existing_descriptions.find(definition.parent_window_name);
        if (it == existing_descriptions.end())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window definition '{}' references an unknown window '{}'",
                definition.formatForErrorMessage(),
                definition.parent_window_name);
        }

        const auto & parent = it->second;
        desc.partition_by = parent.partition_by;
        desc.order_by = parent.order_by;
        desc.frame = parent.frame;

        // If an existing_window_name is specified it must refer to an earlier
        // entry in the WINDOW list; the new window copies its partitioning clause
        // from that entry, as well as its ordering clause if any. In this case
        // the new window cannot specify its own PARTITION BY clause, and it can
        // specify ORDER BY only if the copied window does not have one. The new
        // window always uses its own frame clause; the copied window must not
        // specify a frame clause.
        // -- https://www.postgresql.org/docs/current/sql-select.html
        if (definition.partition_by)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Derived window definition '{}' is not allowed to override PARTITION BY",
                definition.formatForErrorMessage());
        }

        if (definition.order_by && !parent.order_by.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Derived window definition '{}' is not allowed to override a non-empty ORDER BY",
                definition.formatForErrorMessage());
        }

        if (!parent.frame.is_default)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parent window '{}' is not allowed to define a frame: while processing derived window definition '{}'",
                definition.parent_window_name,
                definition.formatForErrorMessage());
        }
    }

    if (definition.partition_by)
    {
        for (const auto & column_ast : definition.partition_by->children)
        {
            const auto * with_alias = dynamic_cast<const ASTWithAlias *>(
                column_ast.get());
            if (!with_alias)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Expected a column in PARTITION BY in window definition,"
                    " got '{}'",
                    column_ast->formatForErrorMessage());
            }
            desc.partition_by.push_back(SortColumnDescription(
                    with_alias->getColumnName(), 1 /* direction */,
                    1 /* nulls_direction */));

            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(column_ast, false, actions_dag);
            desc.partition_by_actions.push_back(std::move(actions_dag));
        }
    }

    if (definition.order_by)
    {
        for (const auto & column_ast
            : definition.order_by->children)
        {
            // Parser should have checked that we have a proper element here.
            const auto & order_by_element
                = column_ast->as<ASTOrderByElement &>();
            // Ignore collation for now.
            desc.order_by.push_back(
                SortColumnDescription(
                    order_by_element.children.front()->getColumnName(),
                    order_by_element.direction,
                    order_by_element.nulls_direction));

            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(column_ast, false, actions_dag);
            desc.order_by_actions.push_back(std::move(actions_dag));
        }
    }

    desc.full_sort_description = desc.partition_by;
    desc.full_sort_description.insert(desc.full_sort_description.end(),
        desc.order_by.begin(), desc.order_by.end());

    if (definition.frame_type != WindowFrame::FrameType::ROWS
        && definition.frame_type != WindowFrame::FrameType::RANGE)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Window frame '{}' is not implemented (while processing '{}')",
            definition.frame_type,
            ast->formatForErrorMessage());
    }

    desc.frame.is_default = definition.frame_is_default;
    desc.frame.type = definition.frame_type;
    desc.frame.begin_type = definition.frame_begin_type;
    desc.frame.begin_preceding = definition.frame_begin_preceding;
    desc.frame.end_type = definition.frame_end_type;
    desc.frame.end_preceding = definition.frame_end_preceding;

    if (definition.frame_end_type == WindowFrame::BoundaryType::Offset)
    {
        auto [value, _] = evaluateConstantExpression(definition.frame_end_offset,
            context_.shared_from_this());
        desc.frame.end_offset = value;
    }

    if (definition.frame_begin_type == WindowFrame::BoundaryType::Offset)
    {
        auto [value, _] = evaluateConstantExpression(definition.frame_begin_offset,
            context_.shared_from_this());
        desc.frame.begin_offset = value;
    }
}

void ExpressionAnalyzer::makeWindowDescriptions(ActionsDAGPtr actions)
{
    auto current_context = getContext();

    // Window definitions from the WINDOW clause
    const auto * select_query = query->as<ASTSelectQuery>();
    if (select_query && select_query->window())
    {
        for (const auto & ptr : select_query->window()->children)
        {
            const auto & elem = ptr->as<const ASTWindowListElement &>();
            WindowDescription desc;
            desc.window_name = elem.name;
            makeWindowDescriptionFromAST(*current_context, window_descriptions,
                desc, elem.definition.get());

            auto [it, inserted] = window_descriptions.insert(
                {desc.window_name, desc});

            if (!inserted)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window '{}' is defined twice in the WINDOW clause",
                    desc.window_name);
            }
        }
    }

    // Window functions
    for (const ASTFunction * function_node : syntax->window_function_asts)
    {
        assert(function_node->is_window_function);

        WindowFunctionDescription window_function;
        window_function.function_node = function_node;
        window_function.column_name
            = window_function.function_node->getColumnName();
        window_function.function_parameters
            = window_function.function_node->parameters
                ? getAggregateFunctionParametersArray(
                    window_function.function_node->parameters, "", getContext())
                : Array();

        // Requiring a constant reference to a shared pointer to non-const AST
        // doesn't really look sane, but the visitor does indeed require it.
        // Hence we clone the node (not very sane either, I know).
        getRootActionsNoMakeSet(window_function.function_node->clone(), actions);

        const ASTs & arguments
            = window_function.function_node->arguments->children;
        window_function.argument_types.resize(arguments.size());
        window_function.argument_names.resize(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const std::string & name = arguments[i]->getColumnName();
            const auto * node = actions->tryFindInIndex(name);

            if (!node)
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unknown identifier '{}' in window function '{}'",
                    name, window_function.function_node->formatForErrorMessage());
            }

            window_function.argument_types[i] = node->result_type;
            window_function.argument_names[i] = name;
        }

        AggregateFunctionProperties properties;
        window_function.aggregate_function
            = AggregateFunctionFactory::instance().get(
                window_function.function_node->name,
                window_function.argument_types,
                window_function.function_parameters, properties);

        // Find the window corresponding to this function. It may be either
        // referenced by name and previously defined in WINDOW clause, or it
        // may be defined inline.
        if (!function_node->window_name.empty())
        {
            auto it = window_descriptions.find(function_node->window_name);
            if (it == std::end(window_descriptions))
            {
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Window '{}' is not defined (referenced by '{}')",
                    function_node->window_name,
                    function_node->formatForErrorMessage());
            }

            it->second.window_functions.push_back(window_function);
        }
        else
        {
            const auto & definition = function_node->window_definition->as<
                const ASTWindowDefinition &>();
            WindowDescription desc;
            desc.window_name = definition.getDefaultWindowName();
            makeWindowDescriptionFromAST(*current_context, window_descriptions,
                desc, &definition);

            auto [it, inserted] = window_descriptions.insert(
                {desc.window_name, desc});

            if (!inserted)
            {
                assert(it->second.full_sort_description
                    == desc.full_sort_description);
            }

            it->second.window_functions.push_back(window_function);
        }
    }

    bool compile_sort_description = current_context->getSettingsRef().compile_sort_description;
    size_t min_count_to_compile_sort_description = current_context->getSettingsRef().min_count_to_compile_sort_description;

    for (auto & [_, window_description] : window_descriptions)
    {
        window_description.full_sort_description.compile_sort_description = compile_sort_description;
        window_description.full_sort_description.min_count_to_compile_sort_description = min_count_to_compile_sort_description;

        window_description.partition_by.compile_sort_description = compile_sort_description;
        window_description.partition_by.min_count_to_compile_sort_description = min_count_to_compile_sort_description;
    }
}


const ASTSelectQuery * ExpressionAnalyzer::getSelectQuery() const
{
    const auto * select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
    return select_query;
}

const ASTSelectQuery * SelectQueryExpressionAnalyzer::getAggregatingQuery() const
{
    if (!has_aggregation)
        throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
    return getSelectQuery();
}

/// "Big" ARRAY JOIN.
ArrayJoinActionPtr ExpressionAnalyzer::addMultipleArrayJoinAction(ActionsDAGPtr & actions, bool array_join_is_left) const
{
    NameSet result_columns;
    for (const auto & result_source : syntax->array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
        {
            const auto & node = actions->findInIndex(result_source.second);
            actions->getIndex().push_back(&actions->addAlias(node, result_source.first));
        }

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    return std::make_shared<ArrayJoinAction>(result_columns, array_join_is_left, getContext());
}

ArrayJoinActionPtr SelectQueryExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, ActionsDAGPtr & before_array_join, bool only_types)
{
    const auto * select_query = getSelectQuery();

    auto [array_join_expression_list, is_array_join_left] = select_query->arrayJoinExpressionList();
    if (!array_join_expression_list)
        return nullptr;

    ExpressionActionsChain::Step & step = chain.lastStep(sourceColumns());

    getRootActions(array_join_expression_list, only_types, step.actions());

    auto array_join = addMultipleArrayJoinAction(step.actions(), is_array_join_left);
    before_array_join = chain.getLastActions();

    chain.steps.push_back(std::make_unique<ExpressionActionsChain::ArrayJoinStep>(array_join, step.getResultColumns()));

    chain.addStep();

    return array_join;
}

bool SelectQueryExpressionAnalyzer::appendJoinLeftKeys(ExpressionActionsChain & chain, bool only_types)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);

    getRootActions(analyzedJoin().leftKeysList(), only_types, step.actions());
    return true;
}

JoinPtr SelectQueryExpressionAnalyzer::appendJoin(
    ExpressionActionsChain & chain,
    ActionsDAGPtr & converting_join_columns)
{
    const ColumnsWithTypeAndName & left_sample_columns = chain.getLastStep().getResultColumns();

    JoinPtr join = makeJoin(*syntax->ast_join, left_sample_columns, converting_join_columns);

    if (converting_join_columns)
    {
        chain.steps.push_back(std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(converting_join_columns));
        chain.addStep();
    }

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);
    chain.steps.push_back(std::make_unique<ExpressionActionsChain::JoinStep>(
        syntax->analyzed_join, join, step.getResultColumns()));
    chain.addStep();
    return join;
}

static ActionsDAGPtr createJoinedBlockActions(ContextPtr context, const TableJoin & analyzed_join)
{
    ASTPtr expression_list = analyzed_join.rightKeysList();
    auto syntax_result = TreeRewriter(context).analyze(expression_list, analyzed_join.columnsFromJoinedTable());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActionsDAG(true, false);
}

std::shared_ptr<DirectKeyValueJoin> tryKeyValueJoin(std::shared_ptr<TableJoin> analyzed_join, const Block & right_sample_block);

static std::shared_ptr<IJoin> chooseJoinAlgorithm(std::shared_ptr<TableJoin> analyzed_join, std::unique_ptr<QueryPlan> & joined_plan, ContextPtr context)
{
    Block right_sample_block = joined_plan->getCurrentDataStream().header;

    if (analyzed_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT))
    {
        if (JoinPtr kvjoin = tryKeyValueJoin(analyzed_join, right_sample_block))
        {
            /// Do not need to execute plan for right part
            joined_plan.reset();
            return kvjoin;
        }

        /// It's not a hash join actually, that's why we check JoinAlgorithm::DIRECT
        /// It's would be fixed in https://github.com/ClickHouse/ClickHouse/pull/38956
        if (analyzed_join->tryInitDictJoin(right_sample_block, context))
        {
            joined_plan.reset();
            return std::make_shared<HashJoin>(analyzed_join, right_sample_block);
        }
    }

    if (analyzed_join->isEnabledAlgorithm(JoinAlgorithm::PARTIAL_MERGE) ||
        analyzed_join->isEnabledAlgorithm(JoinAlgorithm::PREFER_PARTIAL_MERGE))
    {
        if (MergeJoin::isSupported(analyzed_join))
            return std::make_shared<MergeJoin>(analyzed_join, right_sample_block);
    }

    if (analyzed_join->isEnabledAlgorithm(JoinAlgorithm::HASH) ||
        /// partial_merge is preferred, but can't be used for specified kind of join, fallback to hash
        analyzed_join->isEnabledAlgorithm(JoinAlgorithm::PREFER_PARTIAL_MERGE) ||
        analyzed_join->isEnabledAlgorithm(JoinAlgorithm::PARALLEL_HASH))
    {
        if (analyzed_join->allowParallelHashJoin())
            return std::make_shared<ConcurrentHashJoin>(context, analyzed_join, context->getSettings().max_threads, right_sample_block);
        return std::make_shared<HashJoin>(analyzed_join, right_sample_block);
    }

    if (analyzed_join->isEnabledAlgorithm(JoinAlgorithm::FULL_SORTING_MERGE))
    {
        if (FullSortingMergeJoin::isSupported(analyzed_join))
            return std::make_shared<FullSortingMergeJoin>(analyzed_join, right_sample_block);
    }

    if (analyzed_join->isEnabledAlgorithm(JoinAlgorithm::AUTO))
        return std::make_shared<JoinSwitcher>(analyzed_join, right_sample_block);

    throw Exception("Can't execute any of specified algorithms for specified strictness/kind and right storage type",
                     ErrorCodes::NOT_IMPLEMENTED);
}

static std::unique_ptr<QueryPlan> buildJoinedPlan(
    ContextPtr context,
    const ASTTablesInSelectQueryElement & join_element,
    TableJoin & analyzed_join,
    SelectQueryOptions query_options)
{
    /// Actions which need to be calculated on joined block.
    auto joined_block_actions = createJoinedBlockActions(context, analyzed_join);
    NamesWithAliases required_columns_with_aliases = analyzed_join.getRequiredColumns(
        Block(joined_block_actions->getResultColumns()), joined_block_actions->getRequiredColumns().getNames());

    Names original_right_column_names;
    for (auto & pr : required_columns_with_aliases)
        original_right_column_names.push_back(pr.first);

    /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
        * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
        *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
        * - this function shows the expression JOIN _data1.
        * - JOIN tables will need aliases to correctly resolve USING clause.
        */
    auto interpreter = interpretSubquery(
        join_element.table_expression,
        context,
        original_right_column_names,
        query_options.copy().setWithAllColumns().ignoreProjections(false).ignoreAlias(false));
    auto joined_plan = std::make_unique<QueryPlan>();
    interpreter->buildQueryPlan(*joined_plan);
    {
        Block original_right_columns = interpreter->getSampleBlock();
        auto rename_dag = std::make_unique<ActionsDAG>(original_right_columns.getColumnsWithTypeAndName());
        for (const auto & name_with_alias : required_columns_with_aliases)
        {
            if (name_with_alias.first != name_with_alias.second && original_right_columns.has(name_with_alias.first))
            {
                auto pos = original_right_columns.getPositionByName(name_with_alias.first);
                const auto & alias = rename_dag->addAlias(*rename_dag->getInputs()[pos], name_with_alias.second);
                rename_dag->getIndex()[pos] = &alias;
            }
        }

        auto rename_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), std::move(rename_dag));
        rename_step->setStepDescription("Rename joined columns");
        joined_plan->addStep(std::move(rename_step));
    }

    auto joined_actions_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), std::move(joined_block_actions));
    joined_actions_step->setStepDescription("Joined actions");
    joined_plan->addStep(std::move(joined_actions_step));

    return joined_plan;
}

std::shared_ptr<DirectKeyValueJoin> tryKeyValueJoin(std::shared_ptr<TableJoin> analyzed_join, const Block & right_sample_block)
{
    if (!analyzed_join->isEnabledAlgorithm(JoinAlgorithm::DIRECT))
        return nullptr;

    auto storage = analyzed_join->getStorageKeyValue();
    if (!storage)
    {
        return nullptr;
    }

    if (!isInnerOrLeft(analyzed_join->kind()))
    {
        return nullptr;
    }

    if (analyzed_join->strictness() != ASTTableJoin::Strictness::All &&
        analyzed_join->strictness() != ASTTableJoin::Strictness::Any &&
        analyzed_join->strictness() != ASTTableJoin::Strictness::RightAny)
    {
        return nullptr;
    }

    const auto & clauses = analyzed_join->getClauses();
    bool only_one_key = clauses.size() == 1 &&
        clauses[0].key_names_left.size() == 1 &&
        clauses[0].key_names_right.size() == 1 &&
        !clauses[0].on_filter_condition_left &&
        !clauses[0].on_filter_condition_right;

    if (!only_one_key)
    {
        return nullptr;
    }

    String key_name = clauses[0].key_names_right[0];
    String original_key_name = analyzed_join->getOriginalName(key_name);
    const auto & storage_primary_key = storage->getPrimaryKey();
    if (storage_primary_key.size() != 1 || storage_primary_key[0] != original_key_name)
    {
        return nullptr;
    }

    return std::make_shared<DirectKeyValueJoin>(analyzed_join, right_sample_block, storage);
}

JoinPtr SelectQueryExpressionAnalyzer::makeJoin(
    const ASTTablesInSelectQueryElement & join_element,
    const ColumnsWithTypeAndName & left_columns,
    ActionsDAGPtr & left_convert_actions)
{
    /// Two JOINs are not supported with the same subquery, but different USINGs.

    if (joined_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table join was already created for query");

    ActionsDAGPtr right_convert_actions = nullptr;

    const auto & analyzed_join = syntax->analyzed_join;

    if (auto storage = analyzed_join->getStorageJoin())
    {
        auto right_columns = storage->getRightSampleBlock().getColumnsWithTypeAndName();
        std::tie(left_convert_actions, right_convert_actions) = analyzed_join->createConvertingActions(left_columns, right_columns);
        return storage->getJoinLocked(analyzed_join, getContext());
    }

    joined_plan = buildJoinedPlan(getContext(), join_element, *analyzed_join, query_options);

    const ColumnsWithTypeAndName & right_columns = joined_plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
    std::tie(left_convert_actions, right_convert_actions) = analyzed_join->createConvertingActions(left_columns, right_columns);
    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        joined_plan->addStep(std::move(converting_step));
    }

    JoinPtr join = chooseJoinAlgorithm(analyzed_join, joined_plan, getContext());

    return join;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendPrewhere(
    ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();
    if (!select_query->prewhere())
        return nullptr;

    Names first_action_names;
    if (!chain.steps.empty())
        first_action_names = chain.steps.front()->getRequiredColumns().getNames();

    auto & step = chain.lastStep(sourceColumns());
    getRootActions(select_query->prewhere(), only_types, step.actions());
    String prewhere_column_name = select_query->prewhere()->getColumnName();
    step.addRequiredOutput(prewhere_column_name);

    const auto & node = step.actions()->findInIndex(prewhere_column_name);
    auto filter_type = node.result_type;
    if (!filter_type->canBeUsedInBooleanContext())
        throw Exception("Invalid type for filter in PREWHERE: " + filter_type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    ActionsDAGPtr prewhere_actions;
    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions_dag = std::make_shared<ActionsDAG>(sourceColumns());
        getRootActions(select_query->prewhere(), only_types, tmp_actions_dag);
        /// Constants cannot be removed since they can be used in other parts of the query.
        /// And if they are not used anywhere, except PREWHERE, they will be removed on the next step.
        tmp_actions_dag->removeUnusedActions(
            NameSet{prewhere_column_name},
            /* allow_remove_inputs= */ true,
            /* allow_constant_folding= */ false);

        auto required_columns = tmp_actions_dag->getRequiredColumnsNames();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());
        required_source_columns.insert(first_action_names.begin(), first_action_names.end());

        auto names = step.actions()->getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : sourceColumns())
            if (!required_source_columns.contains(column.name))
                name_set.erase(column.name);

        Names required_output(name_set.begin(), name_set.end());
        prewhere_actions = chain.getLastActions();
        prewhere_actions->removeUnusedActions(required_output);
    }

    {
        /// Add empty action with input = {prewhere actions output} + {unused source columns}
        /// Reasons:
        /// 1. Remove remove source columns which are used only in prewhere actions during prewhere actions execution.
        ///    Example: select A prewhere B > 0. B can be removed at prewhere step.
        /// 2. Store side columns which were calculated during prewhere actions execution if they are used.
        ///    Example: select F(A) prewhere F(A) > 0. F(A) can be saved from prewhere step.
        /// 3. Check if we can remove filter column at prewhere step. If we can, action will store single REMOVE_COLUMN.
        ColumnsWithTypeAndName columns = prewhere_actions->getResultColumns();
        auto required_columns = prewhere_actions->getRequiredColumns();
        NameSet prewhere_input_names;
        NameSet unused_source_columns;

        for (const auto & col : required_columns)
            prewhere_input_names.insert(col.name);

        for (const auto & column : sourceColumns())
        {
            if (!prewhere_input_names.contains(column.name))
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(
            std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(std::make_shared<ActionsDAG>(std::move(columns))));
        chain.steps.back()->additional_input = std::move(unused_source_columns);
        chain.getLastActions();
        chain.addStep();
    }

    return prewhere_actions;
}

bool SelectQueryExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->where())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    getRootActions(select_query->where(), only_types, step.actions());

    auto where_column_name = select_query->where()->getColumnName();
    step.addRequiredOutput(where_column_name);

    const auto & node = step.actions()->findInIndex(where_column_name);
    auto filter_type = node.result_type;
    if (!filter_type->canBeUsedInBooleanContext())
        throw Exception("Invalid type for filter in WHERE: " + filter_type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    return true;
}

bool SelectQueryExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types, bool optimize_aggregation_in_order,
                                                  ManyExpressionActions & group_by_elements_actions)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->groupBy())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    ASTs asts = select_query->groupBy()->children;
    if (select_query->group_by_with_grouping_sets)
    {
        for (const auto & ast : asts)
        {
            for (const auto & ast_element : ast->children)
            {
                step.addRequiredOutput(ast_element->getColumnName());
                getRootActions(ast_element, only_types, step.actions());
            }
        }
    }
    else
    {
        for (const auto & ast : asts)
        {
            step.addRequiredOutput(ast->getColumnName());
            getRootActions(ast, only_types, step.actions());
        }
    }

    if (optimize_aggregation_in_order)
    {
        for (auto & child : asts)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            group_by_elements_actions.emplace_back(
                std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes)));
        }
    }

    return true;
}

void SelectQueryExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    for (const auto & desc : aggregate_descriptions)
        for (const auto & name : desc.argument_names)
            step.addRequiredOutput(name);

    /// Collect aggregates removing duplicates by node.getColumnName()
    /// It's not clear why we recollect aggregates (for query parts) while we're able to use previously collected ones (for entire query)
    /// @note The original recollection logic didn't remove duplicates.
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(select_query->select());

    if (select_query->having())
        GetAggregatesVisitor(data).visit(select_query->having());

    if (select_query->orderBy())
        GetAggregatesVisitor(data).visit(select_query->orderBy());

    /// TODO: data.aggregates -> aggregates()
    for (const ASTFunction * node : data.aggregates)
        if (node->arguments)
            for (auto & argument : node->arguments->children)
                getRootActions(argument, only_types, step.actions());
}

void SelectQueryExpressionAnalyzer::appendWindowFunctionsArguments(
    ExpressionActionsChain & chain, bool /* only_types */)
{
    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    // (1) Add actions for window functions and the columns they require.
    // (2) Mark the columns that are really required. We have to mark them as
    //     required because we finish the expression chain before processing the
    //     window functions.
    // The required columns are:
    //  (a) window function arguments,
    //  (b) the columns from PARTITION BY and ORDER BY.

    // (1a) Actions for PARTITION BY and ORDER BY for windows defined in the
    // WINDOW clause. The inline window definitions will be processed
    // recursively together with (1b) as ASTFunction::window_definition.
    if (getSelectQuery()->window())
    {
        getRootActionsNoMakeSet(getSelectQuery()->window(), step.actions());
    }

    for (const auto & [_, w] : window_descriptions)
    {
        for (const auto & f : w.window_functions)
        {
            // (1b) Actions for function arguments, and also the inline window
            // definitions (1a).
            // Requiring a constant reference to a shared pointer to non-const AST
            // doesn't really look sane, but the visitor does indeed require it.
            getRootActionsNoMakeSet(f.function_node->clone(), step.actions());

            // (2b) Required function argument columns.
            for (const auto & a : f.function_node->arguments->children)
            {
                step.addRequiredOutput(a->getColumnName());
            }
        }

        // (2a) Required PARTITION BY and ORDER BY columns.
        for (const auto & c : w.full_sort_description)
        {
            step.addRequiredOutput(c.column_name);
        }
    }
}

void SelectQueryExpressionAnalyzer::appendExpressionsAfterWindowFunctions(ExpressionActionsChain & chain, bool /* only_types */)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_window);
    for (const auto & expression : syntax->expressions_with_window_function)
    {
        getRootActionsForWindowFunctions(expression->clone(), true, step.actions());
    }
}

void SelectQueryExpressionAnalyzer::appendGroupByModifiers(ActionsDAGPtr & before_aggregation, ExpressionActionsChain & chain, bool /* only_types */)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->groupBy() || !(select_query->group_by_with_rollup || select_query->group_by_with_cube))
        return;

    auto source_columns = before_aggregation->getResultColumns();
    ColumnsWithTypeAndName result_columns;

    for (const auto & source_column : source_columns)
    {
        if (source_column.type->canBeInsideNullable())
            result_columns.emplace_back(makeNullableSafe(source_column.type), source_column.name);
        else
            result_columns.push_back(source_column);
    }
    ExpressionActionsChain::Step & step = chain.lastStep(before_aggregation->getNamesAndTypesList());

    step.actions() = ActionsDAG::makeConvertingActions(source_columns, result_columns, ActionsDAG::MatchColumnsMode::Position);
}

void SelectQueryExpressionAnalyzer::appendSelectSkipWindowExpressions(ExpressionActionsChain::Step & step, ASTPtr const & node)
{
    if (auto * function = node->as<ASTFunction>())
    {
        // Skip window function columns here -- they are calculated after
        // other SELECT expressions by a special step.
        // Also skipping lambda functions because they can't be explicitly evaluated.
        if (function->is_window_function || function->name == "lambda")
            return;
        if (function->compute_after_window_functions)
        {
            for (auto & arg : function->arguments->children)
                appendSelectSkipWindowExpressions(step, arg);
            return;
        }
    }
    step.addRequiredOutput(node->getColumnName());
}

bool SelectQueryExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->having())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActionsForHaving(select_query->having(), only_types, step.actions());
    step.addRequiredOutput(select_query->having()->getColumnName());

    return true;
}

void SelectQueryExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActions(select_query->select(), only_types, step.actions());

    for (const auto & child : select_query->select()->children)
        appendSelectSkipWindowExpressions(step, child);
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types, bool optimize_read_in_order,
                                                           ManyExpressionActions & order_by_elements_actions)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->orderBy())
    {
        auto actions = chain.getLastActions();
        chain.addStep();
        return actions;
    }

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    for (auto & child : select_query->orderBy()->children)
    {
        auto * ast = child->as<ASTOrderByElement>();
        if (!ast || ast->children.empty())
            throw Exception("Bad ORDER BY expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);

        if (getContext()->getSettingsRef().enable_positional_arguments)
        {
            replaceForPositionalArguments(ast->children.at(0), select_query, ASTSelectQuery::Expression::ORDER_BY);
        }
    }

    getRootActions(select_query->orderBy(), only_types, step.actions());

    bool with_fill = false;

    for (auto & child : select_query->orderBy()->children)
    {
        auto * ast = child->as<ASTOrderByElement>();
        ASTPtr order_expression = ast->children.at(0);
        const String & column_name = order_expression->getColumnName();
        step.addRequiredOutput(column_name);
        order_by_keys.emplace(column_name);

        if (ast->with_fill)
            with_fill = true;
    }

    if (auto interpolate_list = select_query->interpolate())
    {

        NameSet select;
        for (const auto & child : select_query->select()->children)
            select.insert(child->getAliasOrColumnName());

        /// collect columns required for interpolate expressions -
        /// interpolate expression can use any available column
        auto find_columns = [&step, &select](IAST * function)
        {
            auto f_impl = [&step, &select](IAST * fn, auto fi)
            {
                if (auto * ident = fn->as<ASTIdentifier>())
                {
                    /// exclude columns from select expression - they are already available
                    if (!select.contains(ident->getColumnName()))
                        step.addRequiredOutput(ident->getColumnName());
                    return;
                }
                if (fn->as<ASTFunction>() || fn->as<ASTExpressionList>())
                    for (const auto & ch : fn->children)
                        fi(ch.get(), fi);
                return;
            };
            f_impl(function, f_impl);
        };

        for (const auto & interpolate : interpolate_list->children)
            find_columns(interpolate->as<ASTInterpolateElement>()->expr.get());
    }

    if (optimize_read_in_order)
    {
        for (const auto & child : select_query->orderBy()->children)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            order_by_elements_actions.emplace_back(
                std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes)));
        }
    }

    NameSet non_constant_inputs;
    if (with_fill)
    {
        for (const auto & column : step.getResultColumns())
            non_constant_inputs.insert(column.name);
    }

    auto actions = chain.getLastActions();
    chain.addStep(non_constant_inputs);
    return actions;
}

bool SelectQueryExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->limitBy())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActions(select_query->limitBy(), only_types, step.actions());

    NameSet existing_column_names;
    for (const auto & column : aggregated_columns)
    {
        step.addRequiredOutput(column.name);
        existing_column_names.insert(column.name);
    }
    /// Columns from ORDER BY could be required to do ORDER BY on the initiator in case of distributed queries.
    for (const auto & column_name : order_by_keys)
    {
        step.addRequiredOutput(column_name);
        existing_column_names.insert(column_name);
    }

    auto & children = select_query->limitBy()->children;
    for (auto & child : children)
    {
        if (getContext()->getSettingsRef().enable_positional_arguments)
            replaceForPositionalArguments(child, select_query, ASTSelectQuery::Expression::LIMIT_BY);

        auto child_name = child->getColumnName();
        if (!existing_column_names.contains(child_name))
            step.addRequiredOutput(child_name);
    }

    return true;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    NamesWithAliases result_columns;

    ASTs asts = select_query->select()->children;
    for (const auto & ast : asts)
    {
        String result_name = ast->getAliasOrColumnName();
        if (required_result_columns.empty() || required_result_columns.contains(result_name))
        {
            std::string source_name = ast->getColumnName();

            /*
             * For temporary columns created by ExpressionAnalyzer for literals,
             * use the correct source column. Using the default display name
             * returned by getColumnName is not enough, and we have to use the
             * column id set by EA. In principle, this logic applies to all kinds
             * of columns, not only literals. Literals are especially problematic
             * for two reasons:
             * 1) confusing different literal columns leads to weird side
             *    effects (see 01101_literal_columns_clash);
             * 2) the disambiguation mechanism in SyntaxAnalyzer, that, among
             *    other things, creates unique aliases for columns with same
             *    names from different tables, is applied before these temporary
             *    columns are created by ExpressionAnalyzer.
             * Similar problems should also manifest for function columns, which
             * are likewise created at a later stage by EA.
             * In general, we need to have explicit separation between display
             * names and identifiers for columns. This code is a workaround for
             * a particular subclass of problems, and not a proper solution.
             */
            if (const auto * as_literal = ast->as<ASTLiteral>())
            {
                source_name = as_literal->unique_column_name;
                assert(!source_name.empty());
            }

            result_columns.emplace_back(source_name, result_name);
            step.addRequiredOutput(result_columns.back().second);
        }
    }

    auto actions = chain.getLastActions();
    actions->project(result_columns);
    return actions;
}


void ExpressionAnalyzer::appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types)
{
    ExpressionActionsChain::Step & step = chain.lastStep(sourceColumns());
    getRootActions(expr, only_types, step.actions());
    step.addRequiredOutput(expr->getColumnName());
}


ActionsDAGPtr ExpressionAnalyzer::getActionsDAG(bool add_aliases, bool project_result)
{
    auto actions_dag = std::make_shared<ActionsDAG>(aggregated_columns);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (const auto * node = query->as<ASTExpressionList>())
        asts = node->children;
    else
        asts = ASTs(1, query);

    for (const auto & ast : asts)
    {
        std::string name = ast->getColumnName();
        std::string alias;
        if (add_aliases)
            alias = ast->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(ast, false /* no_makeset_for_subqueries */, actions_dag);
    }

    if (add_aliases)
    {
        if (project_result)
            actions_dag->project(result_columns);
        else
            actions_dag->addAliases(result_columns);
    }

    if (!(add_aliases && project_result))
    {
        NameSet name_set(result_names.begin(), result_names.end());
        /// We will not delete the original columns.
        for (const auto & column_name_type : sourceColumns())
        {
            if (!name_set.contains(column_name_type.name))
            {
                result_names.push_back(column_name_type.name);
                name_set.insert(column_name_type.name);
            }
        }

        actions_dag->removeUnusedActions(name_set);
    }

    return actions_dag;
}

ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result, CompileExpressions compile_expressions)
{
    return std::make_shared<ExpressionActions>(
        getActionsDAG(add_aliases, project_result), ExpressionActionsSettings::fromContext(getContext(), compile_expressions));
}

ExpressionActionsPtr ExpressionAnalyzer::getConstActions(const ColumnsWithTypeAndName & constant_inputs)
{
    auto actions = std::make_shared<ActionsDAG>(constant_inputs);

    getRootActions(query, true /* no_makeset_for_subqueries */, actions, true /* only_consts */);
    return std::make_shared<ExpressionActions>(actions, ExpressionActionsSettings::fromContext(getContext()));
}

std::unique_ptr<QueryPlan> SelectQueryExpressionAnalyzer::getJoinedPlan()
{
    return std::move(joined_plan);
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::simpleSelectActions()
{
    ExpressionActionsChain new_chain(getContext());
    appendSelect(new_chain, false);
    return new_chain.getLastActions();
}

ExpressionAnalysisResult::ExpressionAnalysisResult(
    SelectQueryExpressionAnalyzer & query_analyzer,
    const StorageMetadataPtr & metadata_snapshot,
    bool first_stage_,
    bool second_stage_,
    bool only_types,
    const FilterDAGInfoPtr & filter_info_,
    const FilterDAGInfoPtr & additional_filter,
    const Block & source_header)
    : first_stage(first_stage_)
    , second_stage(second_stage_)
    , need_aggregate(query_analyzer.hasAggregation())
    , has_window(query_analyzer.hasWindow())
    , use_grouping_set_key(query_analyzer.useGroupingSetKey())
{
    /// first_stage: Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    /// second_stage: Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.

    /** First we compose a chain of actions and remember the necessary steps from it.
        *  Regardless of from_stage and to_stage, we will compose a complete sequence of actions to perform optimization and
        *  throw out unnecessary columns based on the entire query. In unnecessary parts of the query, we will not execute subqueries.
        */

    const ASTSelectQuery & query = *query_analyzer.getSelectQuery();
    auto context = query_analyzer.getContext();
    const Settings & settings = context->getSettingsRef();
    const ConstStoragePtr & storage = query_analyzer.storage();

    Names additional_required_columns_after_prewhere;
    ssize_t prewhere_step_num = -1;
    ssize_t where_step_num = -1;
    ssize_t having_step_num = -1;

    auto finalize_chain = [&](ExpressionActionsChain & chain)
    {
        if (prewhere_step_num >= 0)
        {
            ExpressionActionsChain::Step & step = *chain.steps.at(prewhere_step_num);

            auto required_columns = prewhere_info->prewhere_actions->getRequiredColumnsNames();
            NameSet required_source_columns(required_columns.begin(), required_columns.end());
            /// Add required columns to required output in order not to remove them after prewhere execution.
            /// TODO: add sampling and final execution to common chain.
            for (const auto & column : additional_required_columns_after_prewhere)
            {
                if (required_source_columns.contains(column))
                    step.addRequiredOutput(column);
            }
        }

        chain.finalize();

        finalize(chain, prewhere_step_num, where_step_num, having_step_num, query);

        chain.clear();
    };

    {
        ExpressionActionsChain chain(context);

        if (storage && (query.sampleSize() || settings.parallel_replicas_count > 1))
        {
            Names columns_for_sampling = metadata_snapshot->getColumnsRequiredForSampling();
            additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                columns_for_sampling.begin(), columns_for_sampling.end());
        }

        if (storage && query.final())
        {
            Names columns_for_final = metadata_snapshot->getColumnsRequiredForFinal();
            additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                columns_for_final.begin(), columns_for_final.end());
        }

        if (storage && additional_filter)
        {
            Names columns_for_additional_filter = additional_filter->actions->getRequiredColumnsNames();
            additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                columns_for_additional_filter.begin(), columns_for_additional_filter.end());
        }

        if (storage && filter_info_)
        {
            filter_info = filter_info_;
            filter_info->do_remove_column = true;
        }

        if (auto actions = query_analyzer.appendPrewhere(chain, !first_stage))
        {
            /// Prewhere is always the first one.
            prewhere_step_num = 0;
            prewhere_info = std::make_shared<PrewhereInfo>(actions, query.prewhere()->getColumnName());

            if (allowEarlyConstantFolding(*prewhere_info->prewhere_actions, settings))
            {
                Block before_prewhere_sample = source_header;
                if (sanitizeBlock(before_prewhere_sample))
                {
                    ExpressionActions(
                        prewhere_info->prewhere_actions,
                        ExpressionActionsSettings::fromSettings(context->getSettingsRef())).execute(before_prewhere_sample);
                    auto & column_elem = before_prewhere_sample.getByName(query.prewhere()->getColumnName());
                    /// If the filter column is a constant, record it.
                    if (column_elem.column)
                        prewhere_constant_filter_description = ConstantFilterDescription(*column_elem.column);
                }
            }
        }

        array_join = query_analyzer.appendArrayJoin(chain, before_array_join, only_types || !first_stage);

        if (query_analyzer.hasTableJoin())
        {
            query_analyzer.appendJoinLeftKeys(chain, only_types || !first_stage);
            before_join = chain.getLastActions();
            join = query_analyzer.appendJoin(chain, converting_join_columns);
            chain.addStep();
        }

        if (query_analyzer.appendWhere(chain, only_types || !first_stage))
        {
            where_step_num = chain.steps.size() - 1;
            before_where = chain.getLastActions();
            if (allowEarlyConstantFolding(*before_where, settings))
            {
                Block before_where_sample;
                if (chain.steps.size() > 1)
                    before_where_sample = Block(chain.steps[chain.steps.size() - 2]->getResultColumns());
                else
                    before_where_sample = source_header;
                if (sanitizeBlock(before_where_sample))
                {
                    ExpressionActions(
                        before_where,
                        ExpressionActionsSettings::fromSettings(context->getSettingsRef())).execute(before_where_sample);
                    auto & column_elem = before_where_sample.getByName(query.where()->getColumnName());
                    /// If the filter column is a constant, record it.
                    if (column_elem.column)
                        where_constant_filter_description = ConstantFilterDescription(*column_elem.column);
                }
            }
            chain.addStep();
        }

        if (need_aggregate)
        {
            /// TODO correct conditions
            optimize_aggregation_in_order =
                    context->getSettingsRef().optimize_aggregation_in_order
                    && storage && query.groupBy();

            query_analyzer.appendGroupBy(chain, only_types || !first_stage, optimize_aggregation_in_order, group_by_elements_actions);
            query_analyzer.appendAggregateFunctionsArguments(chain, only_types || !first_stage);
            before_aggregation = chain.getLastActions();

            if (settings.group_by_use_nulls)
                query_analyzer.appendGroupByModifiers(before_aggregation, chain, only_types);

            finalize_chain(chain);

            if (query_analyzer.appendHaving(chain, only_types || !second_stage))
            {
                having_step_num = chain.steps.size() - 1;
                before_having = chain.getLastActions();
                chain.addStep();
            }
        }

        bool join_allow_read_in_order = true;
        if (hasJoin())
        {
            /// You may find it strange but we support read_in_order for HashJoin and do not support for MergeJoin.
            join_has_delayed_stream = query_analyzer.analyzedJoin().needStreamWithNonJoinedRows();
            join_allow_read_in_order = typeid_cast<HashJoin *>(join.get()) && !join_has_delayed_stream;
        }

        optimize_read_in_order =
            settings.optimize_read_in_order
            && storage
            && query.orderBy()
            && !query_analyzer.hasAggregation()
            && !query_analyzer.hasWindow()
            && !query.final()
            && join_allow_read_in_order;

        if (storage && optimize_read_in_order)
        {
            Names columns_for_sorting_key = metadata_snapshot->getColumnsRequiredForSortingKey();
            additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                columns_for_sorting_key.begin(), columns_for_sorting_key.end());
        }

        /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
        query_analyzer.appendSelect(chain, only_types || (need_aggregate ? !second_stage : !first_stage));

        // Window functions are processed in a separate expression chain after
        // the main SELECT, similar to what we do for aggregate functions.
        if (has_window)
        {
            query_analyzer.makeWindowDescriptions(chain.getLastActions());

            query_analyzer.appendWindowFunctionsArguments(chain, only_types || !first_stage);

            // Build a list of output columns of the window step.
            // 1) We need the columns that are the output of ExpressionActions.
            for (const auto & x : chain.getLastActions()->getNamesAndTypesList())
            {
                query_analyzer.columns_after_window.push_back(x);
            }
            // 2) We also have to manually add the output of the window function
            // to the list of the output columns of the window step, because the
            // window functions are not in the ExpressionActions.
            for (const auto & [_, w] : query_analyzer.window_descriptions)
            {
                for (const auto & f : w.window_functions)
                {
                    query_analyzer.columns_after_window.push_back(
                        {f.column_name, f.aggregate_function->getReturnType()});
                }
            }

            before_window = chain.getLastActions();
            finalize_chain(chain);

            query_analyzer.appendExpressionsAfterWindowFunctions(chain, only_types || !first_stage);
            for (const auto & x : chain.getLastActions()->getNamesAndTypesList())
            {
                query_analyzer.columns_after_window.push_back(x);
            }

            auto & step = chain.lastStep(query_analyzer.columns_after_window);

            // The output of this expression chain is the result of
            // SELECT (before "final projection" i.e. renaming the columns), so
            // we have to mark the expressions that are required in the output,
            // again. We did it for the previous expression chain ("select without
            // window functions") earlier, in appendSelect(). But that chain also
            // produced the expressions required to calculate window functions.
            // They are not needed in the final SELECT result. Knowing the correct
            // list of columns is important when we apply SELECT DISTINCT later.
            const auto * select_query = query_analyzer.getSelectQuery();
            for (const auto & child : select_query->select()->children)
            {
                step.addRequiredOutput(child->getColumnName());
            }
        }

        selected_columns.clear();
        selected_columns.reserve(chain.getLastStep().required_output.size());
        for (const auto & it : chain.getLastStep().required_output)
            selected_columns.emplace_back(it.first);

        has_order_by = query.orderBy() != nullptr;
        before_order_by = query_analyzer.appendOrderBy(
                chain,
                only_types || (need_aggregate ? !second_stage : !first_stage),
                optimize_read_in_order,
                order_by_elements_actions);

        if (query_analyzer.appendLimitBy(chain, only_types || !second_stage))
        {
            before_limit_by = chain.getLastActions();
            chain.addStep();
        }

        final_projection = query_analyzer.appendProjectResult(chain);

        finalize_chain(chain);
    }

    /// Before executing WHERE and HAVING, remove the extra columns from the block (mostly the aggregation keys).
    removeExtraColumns();

    checkActions();
}

void ExpressionAnalysisResult::finalize(
    const ExpressionActionsChain & chain,
    ssize_t & prewhere_step_num,
    ssize_t & where_step_num,
    ssize_t & having_step_num,
    const ASTSelectQuery & query)
{
    if (prewhere_step_num >= 0)
    {
        const ExpressionActionsChain::Step & step = *chain.steps.at(prewhere_step_num);
        prewhere_info->prewhere_actions->projectInput(false);

        NameSet columns_to_remove;
        for (const auto & [name, can_remove] : step.required_output)
        {
            if (name == prewhere_info->prewhere_column_name)
                prewhere_info->remove_prewhere_column = can_remove;
            else if (can_remove)
                columns_to_remove.insert(name);
        }

        columns_to_remove_after_prewhere = std::move(columns_to_remove);
        prewhere_step_num = -1;
    }

    if (where_step_num >= 0)
    {
        where_column_name = query.where()->getColumnName();
        remove_where_filter = chain.steps.at(where_step_num)->required_output.find(where_column_name)->second;
        where_step_num = -1;
    }

    if (having_step_num >= 0)
    {
        having_column_name = query.having()->getColumnName();
        remove_having_filter = chain.steps.at(having_step_num)->required_output.find(having_column_name)->second;
        having_step_num = -1;
    }
}

void ExpressionAnalysisResult::removeExtraColumns() const
{
    if (hasWhere())
        before_where->projectInput();
    if (hasHaving())
        before_having->projectInput();
}

void ExpressionAnalysisResult::checkActions() const
{
    /// Check that PREWHERE doesn't contain unusual actions. Unusual actions are that can change number of rows.
    if (hasPrewhere())
    {
        auto check_actions = [](const ActionsDAGPtr & actions)
        {
            if (actions)
                for (const auto & node : actions->getNodes())
                    if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
                        throw Exception("PREWHERE cannot contain ARRAY JOIN action", ErrorCodes::ILLEGAL_PREWHERE);
        };

        check_actions(prewhere_info->prewhere_actions);
    }
}

std::string ExpressionAnalysisResult::dump() const
{
    WriteBufferFromOwnString ss;

    ss << "need_aggregate " << need_aggregate << "\n";
    ss << "has_order_by " << has_order_by << "\n";
    ss << "has_window " << has_window << "\n";

    if (before_array_join)
    {
        ss << "before_array_join " << before_array_join->dumpDAG() << "\n";
    }

    if (array_join)
    {
        ss << "array_join " << "FIXME doesn't have dump" << "\n";
    }

    if (before_join)
    {
        ss << "before_join " << before_join->dumpDAG() << "\n";
    }

    if (before_where)
    {
        ss << "before_where " << before_where->dumpDAG() << "\n";
    }

    if (prewhere_info)
    {
        ss << "prewhere_info " << prewhere_info->dump() << "\n";
    }

    if (filter_info)
    {
        ss << "filter_info " << filter_info->dump() << "\n";
    }

    if (before_aggregation)
    {
        ss << "before_aggregation " << before_aggregation->dumpDAG() << "\n";
    }

    if (before_having)
    {
        ss << "before_having " << before_having->dumpDAG() << "\n";
    }

    if (before_window)
    {
        ss << "before_window " << before_window->dumpDAG() << "\n";
    }

    if (before_order_by)
    {
        ss << "before_order_by " << before_order_by->dumpDAG() << "\n";
    }

    if (before_limit_by)
    {
        ss << "before_limit_by " << before_limit_by->dumpDAG() << "\n";
    }

    if (final_projection)
    {
        ss << "final_projection " << final_projection->dumpDAG() << "\n";
    }

    if (!selected_columns.empty())
    {
        ss << "selected_columns ";
        for (size_t i = 0; i < selected_columns.size(); ++i)
        {
            if (i > 0)
            {
                ss << ", ";
            }
            ss << backQuote(selected_columns[i]);
        }
        ss << "\n";
    }

    return ss.str();
}

}
