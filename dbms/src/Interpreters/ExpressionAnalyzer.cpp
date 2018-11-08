#include <Poco/Util/Application.h>
#include <Poco/String.h>

#include <Core/Block.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/formatAST.h>
#include <Parsers/DumpASTNode.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/IColumn.h>

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Set.h>
#include <Interpreters/Join.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/ExecuteScalarSubqueriesVisitor.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/LazyBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Dictionaries/IDictionary.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <Parsers/formatAST.h>

#include <ext/range.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Parsers/queryToString.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/QueryNormalizer.h>

#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ExternalTablesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/ArrayJoinedColumnsVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int CYCLIC_ALIASES;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_ELEMENT_OF_SET;
    extern const int ALIAS_REQUIRED;
    extern const int EMPTY_NESTED_TABLE;
    extern const int DUPLICATE_COLUMN;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int ILLEGAL_AGGREGATION;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CONDITIONAL_TREE_PARENT_NOT_FOUND;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int EXPECTED_ALL_OR_ANY;
}


//namespace
//{
//
//void removeDuplicateColumns(NamesAndTypesList & columns)
//{
//    std::set<String> names;
//    for (auto it = columns.begin(); it != columns.end();)
//    {
//        if (names.emplace(it->name).second)
//            ++it;
//        else
//            columns.erase(it++);
//    }
//}
//
//}


ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const Context & context_,
    const StoragePtr & storage_,
    const NamesAndTypesList & source_columns_,
    const Names & required_result_columns_,
    size_t subquery_depth_,
    bool do_global_,
    const SubqueriesForSets & subqueries_for_sets_)
    : ExpressionAnalyzerData(source_columns_, required_result_columns_, subqueries_for_sets_),
    query(query_), context(context_), settings(context.getSettings()), storage(storage_),
    subquery_depth(subquery_depth_), do_global(do_global_)
{
    auto syntax_analyzer_result = SyntaxAnalyzer()
            .analyze(query, context, storage, source_columns, required_result_columns_, subquery_depth);
    query = syntax_analyzer_result.query;
    storage = syntax_analyzer_result.storage;
    source_columns = syntax_analyzer_result.source_columns;
    array_join_result_to_source = syntax_analyzer_result.array_join_result_to_source;
    array_join_alias_to_name = syntax_analyzer_result.array_join_alias_to_name;
    array_join_name_to_alias = syntax_analyzer_result.array_join_name_to_alias;
    analyzed_join = syntax_analyzer_result.analyzed_join;
    rewrite_subqueries = syntax_analyzer_result.rewrite_subqueries;

    select_query = typeid_cast<ASTSelectQuery *>(query.get());

    // removeDuplicateColumns(source_columns);

    /// Delete the unnecessary from `source_columns` list. Create `unknown_required_source_columns`. Form `columns_added_by_join`.
    collectUsedColumns();

    /// external_tables, subqueries_for_sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables();

    /// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
    /// This analysis should be performed after processing global subqueries, because otherwise,
    /// if the aggregate function contains a global subquery, then `analyzeAggregation` method will save
    /// in `aggregate_descriptions` the information about the parameters of this aggregate function, among which
    /// global subquery. Then, when you call `initGlobalSubqueriesAndExternalTables` method, this
    /// the global subquery will be replaced with a temporary table, resulting in aggregate_descriptions
    /// will contain out-of-date information, which will lead to an error when the query is executed.
    analyzeAggregation();
}

bool ExpressionAnalyzer::isRemoteStorage() const
{
    return storage && storage->isRemote();
}


void ExpressionAnalyzer::analyzeAggregation()
{
    /** Find aggregation keys (aggregation_keys), information about aggregate functions (aggregate_descriptions),
     *  as well as a set of columns obtained after the aggregation, if any,
     *  or after all the actions that are usually performed before aggregation (aggregated_columns).
     *
     * Everything below (compiling temporary ExpressionActions) - only for the purpose of query analysis (type output).
     */

    if (select_query && (select_query->group_expression_list || select_query->having_expression))
        has_aggregation = true;

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(source_columns, context);

    if (select_query && select_query->array_join_expression_list())
    {
        getRootActions(select_query->array_join_expression_list(), true, temp_actions);
        addMultipleArrayJoinAction(temp_actions);
        array_join_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
    }

    if (select_query)
    {
        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            const auto table_join = static_cast<const ASTTableJoin &>(*join->table_join);
            if (table_join.using_expression_list)
                getRootActions(table_join.using_expression_list, true, temp_actions);
            if (table_join.on_expression)
                for (const auto & key_ast : analyzed_join.key_asts_left)
                    getRootActions(key_ast, true, temp_actions);

            addJoinAction(temp_actions, true);
        }
    }

    getAggregates(query, temp_actions);

    if (has_aggregation)
    {
        assertSelect();

        /// Find out aggregation keys.
        if (select_query->group_expression_list)
        {
            NameSet unique_keys;
            ASTs & group_asts = select_query->group_expression_list->children;
            for (ssize_t i = 0; i < ssize_t(group_asts.size()); ++i)
            {
                ssize_t size = group_asts.size();
                getRootActions(group_asts[i], true, temp_actions);

                const auto & column_name = group_asts[i]->getColumnName();
                const auto & block = temp_actions->getSampleBlock();

                if (!block.has(column_name))
                    throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                const auto & col = block.getByName(column_name);

                /// Constant expressions have non-null column pointer at this stage.
                if (col.column && col.column->isColumnConst())
                {
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

                NameAndTypePair key{column_name, col.type};

                /// Aggregation keys are uniqued.
                if (!unique_keys.count(key.name))
                {
                    unique_keys.insert(key.name);
                    aggregation_keys.push_back(key);

                    /// Key is no longer needed, therefore we can save a little by moving it.
                    aggregated_columns.push_back(std::move(key));
                }
            }

            if (group_asts.empty())
            {
                select_query->group_expression_list = nullptr;
                has_aggregation = select_query->having_expression || aggregate_descriptions.size();
            }
        }

        for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
        {
            AggregateDescription & desc = aggregate_descriptions[i];
            aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
        }
    }
    else
    {
        aggregated_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
    }
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables()
{
    /// Adds existing external tables (not subqueries) to the external_tables dictionary.
    ExternalTablesVisitor tables_visitor(context, external_tables);
    tables_visitor.visit(query);

    if (do_global)
    {
        GlobalSubqueriesVisitor subqueries_visitor(context, subquery_depth, isRemoteStorage(),
                                                   external_tables, subqueries_for_sets, has_global_subqueries);
        subqueries_visitor.visit(query);
    }
}


void ExpressionAnalyzer::makeSetsForIndex()
{
    if (storage && select_query && storage->supportsIndexForIn())
    {
        if (select_query->where_expression)
            makeSetsForIndexImpl(select_query->where_expression, storage->getSampleBlock());
        if (select_query->prewhere_expression)
            makeSetsForIndexImpl(select_query->prewhere_expression, storage->getSampleBlock());
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name)
{
    BlockIO res = interpretSubquery(subquery_or_table_name, context, subquery_depth + 1, {})->execute();

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true);

    set->setHeader(res.in->getHeader());
    while (Block block = res.in->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    prepared_sets[subquery_or_table_name->range] = std::move(set);
}


void ExpressionAnalyzer::makeSetsForIndexImpl(const ASTPtr & node, const Block & sample_block)
{
    for (auto & child : node->children)
    {
        /// Don't descent into subqueries.
        if (typeid_cast<ASTSubquery *>(child.get()))
            continue;

        /// Don't dive into lambda functions
        const ASTFunction * func = typeid_cast<const ASTFunction *>(child.get());
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndexImpl(child, sample_block);
    }

    const ASTFunction * func = typeid_cast<const ASTFunction *>(node.get());
    if (func && functionIsInOperator(func->name))
    {
        const IAST & args = *func->arguments;

        if (storage && storage->mayBenefitFromIndexForIn(args.children.at(0)))
        {
            const ASTPtr & arg = args.children.at(1);

            if (!prepared_sets.count(arg->range)) /// Not already prepared.
            {
                if (typeid_cast<ASTSubquery *>(arg.get()) || typeid_cast<ASTIdentifier *>(arg.get()))
                {
                    if (settings.use_index_for_in_with_subqueries)
                        tryMakeSetForIndexFromSubquery(arg);
                }
                else
                {
                    NamesAndTypesList temp_columns = source_columns;
                    temp_columns.insert(temp_columns.end(), array_join_columns.begin(), array_join_columns.end());
                    for (const auto & joined_column : columns_added_by_join)
                        temp_columns.push_back(joined_column.name_and_type);
                    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(temp_columns, context);
                    getRootActions(func->arguments->children.at(0), true, temp_actions);

                    Block sample_block_with_calculated_columns = temp_actions->getSampleBlock();
                    if (sample_block_with_calculated_columns.has(args.children.at(0)->getColumnName()))
                        makeExplicitSet(func, sample_block_with_calculated_columns, true, context,
                                        settings.size_limits_for_set, prepared_sets);
                }
            }
        }
    }
}

bool ExpressionAnalyzer::isThereArrayJoin(const ASTPtr & ast)
{
    if (typeid_cast<ASTIdentifier *>(ast.get()))
    {
        return false;
    }
    else if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
    {
        if (node->name == "arrayJoin")
        {
            return true;
        }
        if (functionIsInOrGlobalInOperator(node->name))
        {
            return isThereArrayJoin(node->arguments->children.at(0));
        }
        if (node->name == "indexHint")
        {
            return false;
        }
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        {
            return false;
        }
        for (auto & child : node->arguments->children)
        {
            if (isThereArrayJoin(child))
            {
                return true;
            }
        }
        return false;
    }
    else if (typeid_cast<ASTLiteral *>(ast.get()))
    {
        return false;
    }
    else
    {
        for (auto & child : ast->children)
        {
            if (isThereArrayJoin(child))
            {
                return true;
            }
        }
        return false;
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts)
{
    bool is_conditional_tree = !isThereArrayJoin(ast) && settings.enable_conditional_computation && !only_consts;

    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, is_conditional_tree, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());
    actions_visitor.visit(ast);
    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getActionsFromJoinKeys(const ASTTableJoin & table_join, bool no_subqueries, ExpressionActionsPtr & actions)
{
    bool only_consts = false;
    bool is_conditional_tree = !isThereArrayJoin(query) && settings.enable_conditional_computation && !only_consts;

    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, is_conditional_tree, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());

    if (table_join.using_expression_list)
        actions_visitor.visit(table_join.using_expression_list);
    else if (table_join.on_expression)
    {
        for (const auto & ast : analyzed_join.key_asts_left)
            actions_visitor.visit(ast);
    }

    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions)
{
    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query && (ast.get() == select_query->where_expression.get() || ast.get() == select_query->prewhere_expression.get()))
    {
        assertNoAggregates(ast, "in WHERE or PREWHERE");
        return;
    }

    /// If we are not analyzing a SELECT query, but a separate expression, then there can not be aggregate functions in it.
    if (!select_query)
    {
        assertNoAggregates(ast, "in wrong place");
        return;
    }

    const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());
    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
    {
        has_aggregation = true;
        AggregateDescription aggregate;
        aggregate.column_name = node->getColumnName();

        /// Make unique aggregate functions.
        for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
            if (aggregate_descriptions[i].column_name == aggregate.column_name)
                return;

        const ASTs & arguments = node->arguments->children;
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            /// There can not be other aggregate functions within the aggregate functions.
            assertNoAggregates(arguments[i], "inside another aggregate function");

            getRootActions(arguments[i], true, actions);
            const std::string & name = arguments[i]->getColumnName();
            types[i] = actions->getSampleBlock().getByName(name).type;
            aggregate.argument_names[i] = name;
        }

        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);

        aggregate_descriptions.push_back(aggregate);
    }
    else
    {
        for (const auto & child : ast->children)
            if (!typeid_cast<const ASTSubquery *>(child.get())
                && !typeid_cast<const ASTSelectQuery *>(child.get()))
                getAggregates(child, actions);
    }
}


void ExpressionAnalyzer::assertNoAggregates(const ASTPtr & ast, const char * description)
{
    const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        throw Exception("Aggregate function " + node->getColumnName()
            + " is found " + String(description) + " in query", ErrorCodes::ILLEGAL_AGGREGATION);

    for (const auto & child : ast->children)
        if (!typeid_cast<const ASTSubquery *>(child.get())
            && !typeid_cast<const ASTSelectQuery *>(child.get()))
            assertNoAggregates(child, description);
}


void ExpressionAnalyzer::assertSelect() const
{
    if (!select_query)
        throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::assertAggregation() const
{
    if (!has_aggregation)
        throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const
{
    if (chain.steps.empty())
    {
        chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns, context));
    }
}

/// "Big" ARRAY JOIN.
void ExpressionAnalyzer::addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const
{
    NameSet result_columns;
    for (const auto & result_source : array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
            actions->add(ExpressionAction::copyColumn(result_source.second, result_source.first));

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    actions->add(ExpressionAction::arrayJoin(result_columns, select_query->array_join_is_left(), context));
}

bool ExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->array_join_expression_list())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->array_join_expression_list(), only_types, step.actions);

    addMultipleArrayJoinAction(step.actions);

    return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
    NamesAndTypesList columns_added_by_join_list;
    for (const auto & joined_column : columns_added_by_join)
        columns_added_by_join_list.push_back(joined_column.name_and_type);

    if (only_types)
        actions->add(ExpressionAction::ordinaryJoin(nullptr, analyzed_join.key_names_left, columns_added_by_join_list));
    else
        for (auto & subquery_for_set : subqueries_for_sets)
            if (subquery_for_set.second.join)
                actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, analyzed_join.key_names_left,
                                                            columns_added_by_join_list));
}

bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->join())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    const auto & join_element = static_cast<const ASTTablesInSelectQueryElement &>(*select_query->join());
    auto & join_params = static_cast<ASTTableJoin &>(*join_element.table_join);

    if (join_params.strictness == ASTTableJoin::Strictness::Unspecified && join_params.kind != ASTTableJoin::Kind::Cross)
    {
        if (settings.join_default_strictness == "ANY")
            join_params.strictness = ASTTableJoin::Strictness::Any;
        else if (settings.join_default_strictness == "ALL")
            join_params.strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty", DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    const auto & table_to_join = static_cast<const ASTTableExpression &>(*join_element.table_expression);

    getActionsFromJoinKeys(join_params, only_types, step.actions);

    /// Two JOINs are not supported with the same subquery, but different USINGs.
    auto join_hash = join_element.getTreeHash();

    SubqueryForSet & subquery_for_set = subqueries_for_sets[toString(join_hash.first) + "_" + toString(join_hash.second)];

    /// Special case - if table name is specified on the right of JOIN, then the table has the type Join (the previously prepared mapping).
    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        const auto & identifier = static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name);
        DatabaseAndTableWithAlias database_table(identifier);
        StoragePtr table = context.tryGetTable(database_table.database, database_table.table);

        if (table)
        {
            StorageJoin * storage_join = dynamic_cast<StorageJoin *>(table.get());

            if (storage_join)
            {
                storage_join->assertCompatible(join_params.kind, join_params.strictness);
                /// TODO Check the set of keys.

                JoinPtr & join = storage_join->getJoin();
                subquery_for_set.join = join;
            }
        }
    }

    if (!subquery_for_set.join)
    {
        JoinPtr join = std::make_shared<Join>(
            analyzed_join.key_names_left, analyzed_join.key_names_right, analyzed_join.columns_added_by_join_from_right_keys,
            settings.join_use_nulls, settings.size_limits_for_join,
            join_params.kind, join_params.strictness);

        /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
          * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
          *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
          * - this function shows the expression JOIN _data1.
          */
        if (!subquery_for_set.source)
        {
            ASTPtr table;

            if (table_to_join.subquery)
                table = table_to_join.subquery;
            else if (table_to_join.table_function)
                table = table_to_join.table_function;
            else if (table_to_join.database_and_table_name)
                table = table_to_join.database_and_table_name;

            Names original_columns;
            for (const auto & column : analyzed_join.columns_from_joined_table)
                if (required_columns_from_joined_table.count(column.name_and_type.name))
                    original_columns.emplace_back(column.original_name);

            auto interpreter = interpretSubquery(table, context, subquery_depth, original_columns);
            subquery_for_set.source = std::make_shared<LazyBlockInputStream>(
                interpreter->getSampleBlock(),
                [interpreter]() mutable { return interpreter->execute().in; });
        }

        /// Alias duplicating columns as qualified.
        for (const auto & column : analyzed_join.columns_from_joined_table)
            if (required_columns_from_joined_table.count(column.name_and_type.name))
                subquery_for_set.joined_block_aliases.emplace_back(column.original_name, column.name_and_type.name);

        auto sample_block = subquery_for_set.source->getHeader();
        for (const auto & name_with_alias : subquery_for_set.joined_block_aliases)
        {
            if (sample_block.has(name_with_alias.first))
            {
                auto pos = sample_block.getPositionByName(name_with_alias.first);
                auto column = sample_block.getByPosition(pos);
                sample_block.erase(pos);
                column.name = name_with_alias.second;
                sample_block.insert(std::move(column));
            }
        }

        joined_block_actions->execute(sample_block);

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_set.join = join;
        subquery_for_set.join->setSampleBlock(sample_block);
        subquery_for_set.joined_block_actions = joined_block_actions;
    }

    addJoinAction(step.actions, false);

    return true;
}

bool ExpressionAnalyzer::appendPrewhere(ExpressionActionsChain & chain, bool only_types,
                                        const ASTPtr & sampling_expression, const ASTPtr & primary_expression)
{
    assertSelect();

    if (!select_query->prewhere_expression)
        return false;

    Names additional_required_mergetree_columns;
    if (sampling_expression)
        additional_required_mergetree_columns = ExpressionAnalyzer(sampling_expression, context, storage).getRequiredSourceColumns();
    if (primary_expression)
    {
        auto required_primary_columns = ExpressionAnalyzer(primary_expression, context, storage).getRequiredSourceColumns();
        additional_required_mergetree_columns.insert(additional_required_mergetree_columns.end(),
                                                     required_primary_columns.begin(), required_primary_columns.end());
    }

    initChain(chain, source_columns);
    auto & step = chain.getLastStep();
    getRootActions(select_query->prewhere_expression, only_types, step.actions);
    String prewhere_column_name = select_query->prewhere_expression->getColumnName();
    step.required_output.push_back(prewhere_column_name);
    step.can_remove_required_output.push_back(true);

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions = std::make_shared<ExpressionActions>(source_columns, context);
        getRootActions(select_query->prewhere_expression, only_types, tmp_actions);
        tmp_actions->finalize({prewhere_column_name});
        auto required_columns = tmp_actions->getRequiredColumns();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_mergetree_columns)
        {
            if (required_source_columns.count(column))
            {
                step.required_output.push_back(column);
                step.can_remove_required_output.push_back(true);
            }
        }

        auto names = step.actions->getSampleBlock().getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : source_columns)
            if (required_source_columns.count(column.name) == 0)
                name_set.erase(column.name);

        Names required_output(name_set.begin(), name_set.end());
        step.actions->finalize(required_output);
    }

    {
        /// Add empty action with input = {prewhere actions output} + {unused source columns}
        /// Reasons:
        /// 1. Remove remove source columns which are used only in prewhere actions during prewhere actions execution.
        ///    Example: select A prewhere B > 0. B can be removed at prewhere step.
        /// 2. Store side columns which were calculated during prewhere actions execution if they are used.
        ///    Example: select F(A) prewhere F(A) > 0. F(A) can be saved from prewhere step.
        /// 3. Check if we can remove filter column at prewhere step. If we can, action will store single REMOVE_COLUMN.
        ColumnsWithTypeAndName columns = step.actions->getSampleBlock().getColumnsWithTypeAndName();
        auto required_columns = step.actions->getRequiredColumns();
        NameSet prewhere_input_names(required_columns.begin(), required_columns.end());
        NameSet unused_source_columns;

        for (const auto & column : source_columns)
        {
            if (prewhere_input_names.count(column.name) == 0)
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(std::make_shared<ExpressionActions>(std::move(columns), context));
        chain.steps.back().additional_input = std::move(unused_source_columns);
    }

    return true;
}

bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->where_expression)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where_expression->getColumnName());
    step.can_remove_required_output = {true};

    getRootActions(select_query->where_expression, only_types, step.actions);

    return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->group_expression_list)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->group_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
        getRootActions(asts[i], only_types, step.actions);
    }

    return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
    {
        for (size_t j = 0; j < aggregate_descriptions[i].argument_names.size(); ++j)
        {
            step.required_output.push_back(aggregate_descriptions[i].argument_names[j]);
        }
    }

    getActionsBeforeAggregation(select_query->select_expression_list, step.actions, only_types);

    if (select_query->having_expression)
        getActionsBeforeAggregation(select_query->having_expression, step.actions, only_types);

    if (select_query->order_expression_list)
        getActionsBeforeAggregation(select_query->order_expression_list, step.actions, only_types);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    assertAggregation();

    if (!select_query->having_expression)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->having_expression->getColumnName());
    getRootActions(select_query->having_expression, only_types, step.actions);

    return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select_expression_list, only_types, step.actions);

    for (const auto & child : select_query->select_expression_list->children)
        step.required_output.push_back(child->getColumnName());
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->order_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->order_expression_list, only_types, step.actions);

    ASTs asts = select_query->order_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        ASTOrderByElement * ast = typeid_cast<ASTOrderByElement *>(asts[i].get());
        if (!ast || ast->children.size() < 1)
            throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
        ASTPtr order_expression = ast->children.at(0);
        step.required_output.push_back(order_expression->getColumnName());
    }

    return true;
}

bool ExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    assertSelect();

    if (!select_query->limit_by_expression_list)
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->limit_by_expression_list, only_types, step.actions);

    for (const auto & child : select_query->limit_by_expression_list->children)
        step.required_output.push_back(child->getColumnName());

    return true;
}

void ExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    NamesWithAliases result_columns;

    ASTs asts = select_query->select_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        String result_name = asts[i]->getAliasOrColumnName();
        if (required_result_columns.empty()
            || std::find(required_result_columns.begin(), required_result_columns.end(), result_name) !=  required_result_columns.end())
        {
            result_columns.emplace_back(asts[i]->getColumnName(), result_name);
            step.required_output.push_back(result_columns.back().second);
        }
    }

    step.actions->add(ExpressionAction::project(result_columns));
}


void ExpressionAnalyzer::appendExpression(ExpressionActionsChain & chain, const ASTPtr & expr, bool only_types)
{
    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();
    getRootActions(expr, only_types, step.actions);
    step.required_output.push_back(expr->getColumnName());
}


void ExpressionAnalyzer::getActionsBeforeAggregation(const ASTPtr & ast, ExpressionActionsPtr & actions, bool no_subqueries)
{
    ASTFunction * node = typeid_cast<ASTFunction *>(ast.get());

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        for (auto & argument : node->arguments->children)
            getRootActions(argument, no_subqueries, actions);
    else
        for (auto & child : ast->children)
            getActionsBeforeAggregation(child, actions, no_subqueries);
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result)
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(source_columns, context);
    NamesWithAliases result_columns;
    Names result_names;

    ASTs asts;

    if (auto node = typeid_cast<const ASTExpressionList *>(query.get()))
        asts = node->children;
    else
        asts = ASTs(1, query);

    for (size_t i = 0; i < asts.size(); ++i)
    {
        std::string name = asts[i]->getColumnName();
        std::string alias;
        if (add_aliases)
            alias = asts[i]->getAliasOrColumnName();
        else
            alias = name;
        result_columns.emplace_back(name, alias);
        result_names.push_back(alias);
        getRootActions(asts[i], false, actions);
    }

    if (add_aliases)
    {
        if (project_result)
            actions->add(ExpressionAction::project(result_columns));
        else
            actions->add(ExpressionAction::addAliases(result_columns));
    }

    if (!(add_aliases && project_result))
    {
        /// We will not delete the original columns.
        for (const auto & column_name_type : source_columns)
            result_names.push_back(column_name_type.name);
    }

    actions->finalize(result_names);

    return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(NamesAndTypesList(), context);

    getRootActions(query, true, actions, true);
    return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const
{
    for (const auto & name_and_type : aggregation_keys)
        key_names.emplace_back(name_and_type.name);

    aggregates = aggregate_descriptions;
}

void ExpressionAnalyzer::collectUsedColumns()
{
    /** Calculate which columns are required to execute the expression.
      * Then, delete all other columns from the list of available columns.
      * After execution, columns will only contain the list of columns needed to read from the table.
      */

    NameSet required;
    NameSet ignored;

    NameSet available_columns;
    for (const auto & column : source_columns)
        available_columns.insert(column.name);

    if (select_query && select_query->array_join_expression_list())
    {
        ASTs & expressions = select_query->array_join_expression_list()->children;
        for (size_t i = 0; i < expressions.size(); ++i)
        {
            /// Ignore the top-level identifiers from the ARRAY JOIN section.
            /// Then add them separately.
            if (typeid_cast<ASTIdentifier *>(expressions[i].get()))
            {
                ignored.insert(expressions[i]->getColumnName());
            }
            else
            {
                /// Nothing needs to be ignored for expressions in ARRAY JOIN.
                NameSet empty;
                RequiredSourceColumnsVisitor visitor(available_columns, required, empty, empty, empty);
                visitor.visit(expressions[i]);
            }

            ignored.insert(expressions[i]->getAliasOrColumnName());
        }
    }

    /** You also need to ignore the identifiers of the columns that are obtained by JOIN.
      * (Do not assume that they are required for reading from the "left" table).
      */
    NameSet available_joined_columns;
    for (const auto & joined_column : analyzed_join.available_joined_columns)
        available_joined_columns.insert(joined_column.name_and_type.name);

    NameSet required_joined_columns;

    for (const auto & left_key_ast : analyzed_join.key_asts_left)
    {
        NameSet empty;
        RequiredSourceColumnsVisitor columns_visitor(available_columns, required, ignored, empty, required_joined_columns);
        columns_visitor.visit(left_key_ast);
    }

    RequiredSourceColumnsVisitor columns_visitor(available_columns, required, ignored, available_joined_columns, required_joined_columns);
    columns_visitor.visit(query);

    columns_added_by_join = analyzed_join.available_joined_columns;
    for (auto it = columns_added_by_join.begin(); it != columns_added_by_join.end();)
    {
        if (required_joined_columns.count(it->name_and_type.name))
            ++it;
        else
            columns_added_by_join.erase(it++);
    }

    NameSet source_columns_set;
    for (const auto & type_name : source_columns)
        source_columns_set.insert(type_name.name);
    joined_block_actions = analyzed_join.createJoinedBlockActions(
            source_columns_set, columns_added_by_join, select_query, context, required_columns_from_joined_table);

    /// Some columns from right join key may be used in query. This columns will be appended to block during join.
    for (const auto & right_key_name : analyzed_join.key_names_right)
        if (required_joined_columns.count(right_key_name))
            analyzed_join.columns_added_by_join_from_right_keys.insert(right_key_name);

    /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
    NameSet array_join_sources;
    for (const auto & result_source : array_join_result_to_source)
        array_join_sources.insert(result_source.second);

    for (const auto & column_name_type : source_columns)
        if (array_join_sources.count(column_name_type.name))
            required.insert(column_name_type.name);

    /// You need to read at least one column to find the number of rows.
    if (select_query && required.empty())
        required.insert(ExpressionActions::getSmallestColumn(source_columns));

    NameSet unknown_required_source_columns = required;

    for (NamesAndTypesList::iterator it = source_columns.begin(); it != source_columns.end();)
    {
        unknown_required_source_columns.erase(it->name);

        if (!required.count(it->name))
            source_columns.erase(it++);
        else
            ++it;
    }

    /// If there are virtual columns among the unknown columns. Remove them from the list of unknown and add
    /// in columns list, so that when further processing they are also considered.
    if (storage)
    {
        for (auto it = unknown_required_source_columns.begin(); it != unknown_required_source_columns.end();)
        {
            if (storage->hasColumn(*it))
            {
                source_columns.push_back(storage->getColumn(*it));
                unknown_required_source_columns.erase(it++);
            }
            else
                ++it;
        }
    }

    if (!unknown_required_source_columns.empty())
        throw Exception("Unknown identifier: " + *unknown_required_source_columns.begin(), ErrorCodes::UNKNOWN_IDENTIFIER);
}


Names ExpressionAnalyzer::getRequiredSourceColumns() const
{
    return source_columns.getNames();
}

}
