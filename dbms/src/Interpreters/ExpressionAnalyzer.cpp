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
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/IColumn.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/LogicalExpressionsOptimizer.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Set.h>
#include <Interpreters/Join.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/copyData.h>

#include <Dictionaries/IDictionary.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <ext/range.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/QueryNormalizer.h>

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/ExternalTablesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int ILLEGAL_AGGREGATION;
    extern const int EXPECTED_ALL_OR_ANY;
}

ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
    const Context & context_,
    const NamesAndTypesList & additional_source_columns,
    const NameSet & required_result_columns_,
    size_t subquery_depth_,
    bool do_global_,
    const SubqueriesForSets & subqueries_for_sets_)
    : ExpressionAnalyzerData(syntax_analyzer_result_->source_columns, required_result_columns_, subqueries_for_sets_)
    , query(query_), context(context_), settings(context.getSettings())
    , subquery_depth(subquery_depth_), do_global(do_global_)
    , syntax(syntax_analyzer_result_)
{
    storage = syntax->storage;
    rewrite_subqueries = syntax->rewrite_subqueries;

    if (!additional_source_columns.empty())
    {
        source_columns.insert(source_columns.end(), additional_source_columns.begin(), additional_source_columns.end());
        removeDuplicateColumns(source_columns);
    }

    /// Delete the unnecessary from `source_columns` list. Form `columns_added_by_join`.
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

    auto * select_query = query->as<ASTSelectQuery>();

    if (select_query && (select_query->groupBy() || select_query->having()))
        has_aggregation = true;

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(source_columns, context);

    if (select_query)
    {
        bool is_array_join_left;
        ASTPtr array_join_expression_list = select_query->array_join_expression_list(is_array_join_left);
        if (array_join_expression_list)
        {
            getRootActions(array_join_expression_list, true, temp_actions);
            addMultipleArrayJoinAction(temp_actions, is_array_join_left);
            array_join_columns = temp_actions->getSampleBlock().getNamesAndTypesList();
        }

        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            const auto & table_join = join->table_join->as<ASTTableJoin &>();
            if (table_join.using_expression_list)
                getRootActions(table_join.using_expression_list, true, temp_actions);
            if (table_join.on_expression)
                for (const auto & key_ast : analyzedJoin().key_asts_left)
                    getRootActions(key_ast, true, temp_actions);

            addJoinAction(temp_actions, true);
        }
    }

    getAggregates(query, temp_actions);

    if (has_aggregation)
    {
        assertSelect();

        /// Find out aggregation keys.
        if (select_query->groupBy())
        {
            NameSet unique_keys;
            ASTs & group_asts = select_query->groupBy()->children;
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
                select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
                has_aggregation = select_query->having() || aggregate_descriptions.size();
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
    ExternalTablesVisitor::Data tables_data{context, external_tables};
    ExternalTablesVisitor(tables_data).visit(query);

    if (do_global)
    {
        GlobalSubqueriesVisitor::Data subqueries_data(context, subquery_depth, isRemoteStorage(),
                                                   external_tables, subqueries_for_sets, has_global_subqueries);
        GlobalSubqueriesVisitor(subqueries_data).visit(query);
    }
}


void ExpressionAnalyzer::makeSetsForIndex()
{
    const auto * select_query = query->as<ASTSelectQuery>();

    if (storage && select_query && storage->supportsIndexForIn())
    {
        if (select_query->where())
            makeSetsForIndexImpl(select_query->where());
        if (select_query->prewhere())
            makeSetsForIndexImpl(select_query->prewhere());
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name)
{
    auto set_key = PreparedSetKey::forSubquery(*subquery_or_table_name);
    if (prepared_sets.count(set_key))
        return; /// Already prepared.

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, context, subquery_depth + 1, {});
    BlockIO res = interpreter_subquery->execute();

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true);
    set->setHeader(res.in->getHeader());

    while (Block block = res.in->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    prepared_sets[set_key] = std::move(set);
}


void ExpressionAnalyzer::makeSetsForIndexImpl(const ASTPtr & node)
{
    for (auto & child : node->children)
    {
        /// Don't descend into subqueries.
        if (child->as<ASTSubquery>())
            continue;

        /// Don't descend into lambda functions
        const auto * func = child->as<ASTFunction>();
        if (func && func->name == "lambda")
            continue;

        makeSetsForIndexImpl(child);
    }

    const auto * func = node->as<ASTFunction>();
    if (func && functionIsInOperator(func->name))
    {
        const IAST & args = *func->arguments;

        if (storage && storage->mayBenefitFromIndexForIn(args.children.at(0), context))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTIdentifier>())
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


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());
    actions_visitor.visit(ast);
    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getActionsFromJoinKeys(const ASTTableJoin & table_join, bool no_subqueries, ExpressionActionsPtr & actions)
{
    bool only_consts = false;

    LogAST log;
    ActionsVisitor actions_visitor(context, settings.size_limits_for_set, subquery_depth,
                                   source_columns, actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage(), log.stream());

    if (table_join.using_expression_list)
        actions_visitor.visit(table_join.using_expression_list);
    else if (table_join.on_expression)
    {
        for (const auto & ast : analyzedJoin().key_asts_left)
            actions_visitor.visit(ast);
    }

    actions = actions_visitor.popActionsLevel();
}


void ExpressionAnalyzer::getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    /// There can not be aggregate functions inside the WHERE and PREWHERE.
    if (select_query && (ast.get() == select_query->where().get() || ast.get() == select_query->prewhere().get()))
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

    const auto * node = ast->as<ASTFunction>();
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
            types[i] = recursiveRemoveLowCardinality(actions->getSampleBlock().getByName(name).type);
            aggregate.argument_names[i] = name;
        }

        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);

        aggregate_descriptions.push_back(aggregate);
    }
    else
    {
        for (const auto & child : ast->children)
            if (!child->as<ASTSubquery>() && !child->as<ASTSelectQuery>())
                getAggregates(child, actions);
    }
}


void ExpressionAnalyzer::assertNoAggregates(const ASTPtr & ast, const char * description)
{
    const auto * node = ast->as<ASTFunction>();

    if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
        throw Exception("Aggregate function " + node->getColumnName()
            + " is found " + String(description) + " in query", ErrorCodes::ILLEGAL_AGGREGATION);

    for (const auto & child : ast->children)
        if (!child->as<ASTSubquery>() && !child->as<ASTSelectQuery>())
            assertNoAggregates(child, description);
}


void ExpressionAnalyzer::assertSelect() const
{
    const auto * select_query = query->as<ASTSelectQuery>();

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
void ExpressionAnalyzer::addMultipleArrayJoinAction(ExpressionActionsPtr & actions, bool array_join_is_left) const
{
    NameSet result_columns;
    for (const auto & result_source : syntax->array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
            actions->add(ExpressionAction::copyColumn(result_source.second, result_source.first));

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    actions->add(ExpressionAction::arrayJoin(result_columns, array_join_is_left, context));
}

bool ExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    bool is_array_join_left;
    ASTPtr array_join_expression_list = select_query->array_join_expression_list(is_array_join_left);
    if (!array_join_expression_list)
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(array_join_expression_list, only_types, step.actions);

    addMultipleArrayJoinAction(step.actions, is_array_join_left);

    return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
    NamesAndTypesList columns_added_by_join_list;
    for (const auto & joined_column : columns_added_by_join)
        columns_added_by_join_list.push_back(joined_column.name_and_type);

    if (only_types)
        actions->add(ExpressionAction::ordinaryJoin(nullptr, analyzedJoin().key_names_left, columns_added_by_join_list));
    else
        for (auto & subquery_for_set : subqueries_for_sets)
            if (subquery_for_set.second.join)
                actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, analyzedJoin().key_names_left,
                                                            columns_added_by_join_list));
}

static void appendRequiredColumns(
    NameSet & required_columns, const Block & sample, const Names & key_names_right, const JoinedColumnsList & columns_added_by_join)
{
    for (auto & column : key_names_right)
        if (!sample.has(column))
            required_columns.insert(column);

    for (auto & column : columns_added_by_join)
        if (!sample.has(column.name_and_type.name))
            required_columns.insert(column.name_and_type.name);
}

bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    if (!select_query->join())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    const auto & join_element = select_query->join()->as<ASTTablesInSelectQueryElement &>();
    auto & join_params = join_element.table_join->as<ASTTableJoin &>();

    if (join_params.strictness == ASTTableJoin::Strictness::Unspecified && join_params.kind != ASTTableJoin::Kind::Cross)
    {
        if (settings.join_default_strictness == "ANY")
            join_params.strictness = ASTTableJoin::Strictness::Any;
        else if (settings.join_default_strictness == "ALL")
            join_params.strictness = ASTTableJoin::Strictness::All;
        else
            throw Exception("Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty", DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    const auto & table_to_join = join_element.table_expression->as<ASTTableExpression &>();

    getActionsFromJoinKeys(join_params, only_types, step.actions);

    /// Two JOINs are not supported with the same subquery, but different USINGs.
    auto join_hash = join_element.getTreeHash();

    SubqueryForSet & subquery_for_set = subqueries_for_sets[toString(join_hash.first) + "_" + toString(join_hash.second)];

    /// Special case - if table name is specified on the right of JOIN, then the table has the type Join (the previously prepared mapping).
    /// TODO This syntax does not support specifying a database name.
    if (table_to_join.database_and_table_name)
    {
        DatabaseAndTableWithAlias database_table(table_to_join.database_and_table_name);
        StoragePtr table = context.tryGetTable(database_table.database, database_table.table);

        if (table)
        {
            auto * storage_join = dynamic_cast<StorageJoin *>(table.get());

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
        auto & analyzed_join = analyzedJoin();
        /// Actions which need to be calculated on joined block.
        ExpressionActionsPtr joined_block_actions =
            analyzed_join.createJoinedBlockActions(columns_added_by_join, select_query, context);

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

            Names action_columns = joined_block_actions->getRequiredColumns();
            NameSet required_columns(action_columns.begin(), action_columns.end());

            appendRequiredColumns(
                required_columns, joined_block_actions->getSampleBlock(), analyzed_join.key_names_right, columns_added_by_join);

            Names original_columns = analyzed_join.getOriginalColumnNames(required_columns);

            auto interpreter = interpretSubquery(table, context, subquery_depth, original_columns);
            subquery_for_set.makeSource(interpreter, analyzed_join.columns_from_joined_table, required_columns);
        }

        Block sample_block = subquery_for_set.renamedSampleBlock();
        joined_block_actions->execute(sample_block);

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_set.join = std::make_shared<Join>(analyzedJoin().key_names_right, settings.join_use_nulls,
            settings.size_limits_for_join, join_params.kind, join_params.strictness);
        subquery_for_set.join->setSampleBlock(sample_block);
        subquery_for_set.joined_block_actions = joined_block_actions;
    }

    addJoinAction(step.actions, false);

    return true;
}

bool ExpressionAnalyzer::appendPrewhere(
    ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    if (!select_query->prewhere())
        return false;

    initChain(chain, source_columns);
    auto & step = chain.getLastStep();
    getRootActions(select_query->prewhere(), only_types, step.actions);
    String prewhere_column_name = select_query->prewhere()->getColumnName();
    step.required_output.push_back(prewhere_column_name);
    step.can_remove_required_output.push_back(true);

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions = std::make_shared<ExpressionActions>(source_columns, context);
        getRootActions(select_query->prewhere(), only_types, tmp_actions);
        tmp_actions->finalize({prewhere_column_name});
        auto required_columns = tmp_actions->getRequiredColumns();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_columns)
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
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    if (!select_query->where())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where()->getColumnName());
    step.can_remove_required_output = {true};

    getRootActions(select_query->where(), only_types, step.actions);

    return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertAggregation();

    if (!select_query->groupBy())
        return false;

    initChain(chain, source_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->groupBy()->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
        getRootActions(asts[i], only_types, step.actions);
    }

    return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

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

    getActionsBeforeAggregation(select_query->select(), step.actions, only_types);

    if (select_query->having())
        getActionsBeforeAggregation(select_query->having(), step.actions, only_types);

    if (select_query->orderBy())
        getActionsBeforeAggregation(select_query->orderBy(), step.actions, only_types);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertAggregation();

    if (!select_query->having())
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->having()->getColumnName());
    getRootActions(select_query->having(), only_types, step.actions);

    return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select(), only_types, step.actions);

    for (const auto & child : select_query->select()->children)
        step.required_output.push_back(child->getColumnName());
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    if (!select_query->orderBy())
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->orderBy(), only_types, step.actions);

    for (auto & child : select_query->orderBy()->children)
    {
        const auto * ast = child->as<ASTOrderByElement>();
        if (!ast || ast->children.size() < 1)
            throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
        ASTPtr order_expression = ast->children.at(0);
        step.required_output.push_back(order_expression->getColumnName());
    }

    return true;
}

bool ExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    if (!select_query->limitBy())
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->limitBy(), only_types, step.actions);

    for (const auto & child : select_query->limitBy()->children)
        step.required_output.push_back(child->getColumnName());

    return true;
}

void ExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    const auto * select_query = query->as<ASTSelectQuery>();

    assertSelect();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    NamesWithAliases result_columns;

    ASTs asts = select_query->select()->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        String result_name = asts[i]->getAliasOrColumnName();
        if (required_result_columns.empty() || required_result_columns.count(result_name))
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
    const auto * node = ast->as<ASTFunction>();

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

    if (const auto * node = query->as<ASTExpressionList>())
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

    RequiredSourceColumnsVisitor::Data columns_context;
    RequiredSourceColumnsVisitor(columns_context).visit(query);

    NameSet source_column_names;
    for (const auto & column : source_columns)
        source_column_names.insert(column.name);

    NameSet required = columns_context.requiredColumns();

    if (columns_context.has_table_join)
    {
        const AnalyzedJoin & analyzed_join = analyzedJoin();
        NameSet avaliable_columns;
        for (const auto & name : source_columns)
            avaliable_columns.insert(name.name);

        /** You also need to ignore the identifiers of the columns that are obtained by JOIN.
        * (Do not assume that they are required for reading from the "left" table).
        */
        columns_added_by_join.clear();
        for (const auto & joined_column : analyzed_join.available_joined_columns)
        {
            auto & name = joined_column.name_and_type.name;
            if (avaliable_columns.count(name))
                continue;

            if (required.count(name))
            {
                columns_added_by_join.push_back(joined_column);
                required.erase(name);
            }
        }
    }

    NameSet array_join_sources;
    if (columns_context.has_array_join)
    {
        /// Insert the columns required for the ARRAY JOIN calculation into the required columns list.
        for (const auto & result_source : syntax->array_join_result_to_source)
            array_join_sources.insert(result_source.second);

        for (const auto & column_name_type : source_columns)
            if (array_join_sources.count(column_name_type.name))
                required.insert(column_name_type.name);
    }

    const auto * select_query = query->as<ASTSelectQuery>();

    /// You need to read at least one column to find the number of rows.
    if (select_query && required.empty())
        required.insert(ExpressionActions::getSmallestColumn(source_columns));

    NameSet unknown_required_source_columns = required;

    for (NamesAndTypesList::iterator it = source_columns.begin(); it != source_columns.end();)
    {
        const String & column_name = it->name;
        unknown_required_source_columns.erase(column_name);

        if (!required.count(column_name))
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
    {
        std::stringstream ss;
        ss << "Missing columns:";
        for (const auto & name : unknown_required_source_columns)
            ss << " '" << name << "'";
        ss << " while processing query: '" << query << "'";

        ss << ", required columns:";
        for (const auto & name : columns_context.requiredColumns())
            ss << " '" << name << "'";

        if (!source_column_names.empty())
        {
            ss << ", source columns:";
            for (const auto & name : source_column_names)
                ss << " '" << name << "'";
        }
        else
            ss << ", no source columns";

        if (columns_context.has_table_join)
        {
            ss << ", joined columns:";
            for (const auto & column : analyzedJoin().available_joined_columns)
                ss << " '" << column.name_and_type.name << "'";
        }

        if (!array_join_sources.empty())
        {
            ss << ", arrayJoin columns:";
            for (const auto & name : array_join_sources)
                ss << " '" << name << "'";
        }

        throw Exception(ss.str(), ErrorCodes::UNKNOWN_IDENTIFIER);
    }
}


}
