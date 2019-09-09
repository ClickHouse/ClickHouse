#include <Poco/Util/Application.h>
#include <Poco/String.h>

#include <Core/Block.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
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
#include <Interpreters/AnalyzedJoin.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/copyData.h>
#include <DataStreams/IBlockInputStream.h>

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
#include <Interpreters/GetAggregatesVisitor.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
}

ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const SyntaxAnalyzerResultPtr & syntax_analyzer_result_,
    const Context & context_,
    size_t subquery_depth_,
    bool do_global)
    : query(query_), context(context_), settings(context.getSettings())
    , subquery_depth(subquery_depth_)
    , syntax(syntax_analyzer_result_)
{
    /// external_tables, subqueries_for_sets for global subqueries.
    /// Replaces global subqueries with the generated names of temporary tables that will be sent to remote servers.
    initGlobalSubqueriesAndExternalTables(do_global);

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
    return storage() && storage()->isRemote();
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

    ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(sourceColumns(), context);

    if (select_query)
    {
        bool is_array_join_left;
        ASTPtr array_join_expression_list = select_query->array_join_expression_list(is_array_join_left);
        if (array_join_expression_list)
        {
            getRootActions(array_join_expression_list, true, temp_actions);
            addMultipleArrayJoinAction(temp_actions, is_array_join_left);

            array_join_columns.clear();
            for (auto & column : temp_actions->getSampleBlock().getNamesAndTypesList())
                if (syntax->array_join_result_to_source.count(column.name))
                    array_join_columns.emplace_back(column);
        }

        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            getRootActions(analyzedJoin().leftKeysList(), true, temp_actions);
            addJoinAction(temp_actions);
        }
    }

    has_aggregation = makeAggregateDescriptions(temp_actions);
    if (select_query && (select_query->groupBy() || select_query->having()))
        has_aggregation = true;

    if (has_aggregation)
    {
        getSelectQuery(); /// assertSelect()

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
                if (col.column && isColumnConst(*col.column))
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


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables(bool do_global)
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


void SelectQueryExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name)
{
    auto set_key = PreparedSetKey::forSubquery(*subquery_or_table_name);
    if (prepared_sets.count(set_key))
        return; /// Already prepared.

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, context, subquery_depth + 1, {});
    BlockIO res = interpreter_subquery->execute();

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true);
    set->setHeader(res.in->getHeader());

    res.in->readPrefix();
    while (Block block = res.in->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }
    res.in->readSuffix();

    prepared_sets[set_key] = std::move(set);
}


/// Perfomance optimisation for IN() if storage supports it.
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
    if (func && functionIsInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & left_in_operand = args.children.at(0);

        if (storage()->mayBenefitFromIndexForIn(left_in_operand, context))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTIdentifier>())
            {
                if (settings.use_index_for_in_with_subqueries)
                    tryMakeSetForIndexFromSubquery(arg);
            }
            else
            {
                NamesAndTypesList temp_columns = sourceColumns();
                temp_columns.insert(temp_columns.end(), array_join_columns.begin(), array_join_columns.end());
                temp_columns.insert(temp_columns.end(),
                                    analyzedJoin().columnsAddedByJoin().begin(), analyzedJoin().columnsAddedByJoin().end());

                ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(temp_columns, context);
                getRootActions(left_in_operand, true, temp_actions);

                Block sample_block_with_calculated_columns = temp_actions->getSampleBlock();
                if (sample_block_with_calculated_columns.has(left_in_operand->getColumnName()))
                    makeExplicitSet(func, sample_block_with_calculated_columns, true, context,
                        settings.size_limits_for_set, prepared_sets);
            }
        }
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ExpressionActionsPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(context, settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), actions, prepared_sets, subqueries_for_sets,
                                   no_subqueries, only_consts, !isRemoteStorage());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    visitor_data.updateActions(actions);
}


bool ExpressionAnalyzer::makeAggregateDescriptions(ExpressionActionsPtr & actions)
{
    for (const ASTFunction * node : aggregates())
    {
        AggregateDescription aggregate;
        aggregate.column_name = node->getColumnName();

        const ASTs & arguments = node->arguments->children;
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            getRootActions(arguments[i], true, actions);
            const std::string & name = arguments[i]->getColumnName();
            types[i] = actions->getSampleBlock().getByName(name).type;
            aggregate.argument_names[i] = name;
        }

        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);

        aggregate_descriptions.push_back(aggregate);
    }

    return !aggregates().empty();
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

bool SelectQueryExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    bool is_array_join_left;
    ASTPtr array_join_expression_list = select_query->array_join_expression_list(is_array_join_left);
    if (!array_join_expression_list)
        return false;

    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(array_join_expression_list, only_types, step.actions);

    addMultipleArrayJoinAction(step.actions, is_array_join_left);

    return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions) const
{
    actions->add(ExpressionAction::ordinaryJoin(syntax->analyzed_join));
}

bool SelectQueryExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
    const ASTTablesInSelectQueryElement * ast_join = getSelectQuery()->join();
    if (!ast_join)
        return false;

    makeTableJoin(*ast_join);

    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(analyzedJoin().leftKeysList(), only_types, step.actions);
    addJoinAction(step.actions);
    return true;
}

static JoinPtr tryGetStorageJoin(const ASTTablesInSelectQueryElement & join_element, const Context & context)
{
    const auto & table_to_join = join_element.table_expression->as<ASTTableExpression &>();
    auto & join_params = join_element.table_join->as<ASTTableJoin &>();

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

                return storage_join->getJoin();
            }
        }
    }

    return {};
}

static ExpressionActionsPtr createJoinedBlockActions(const Context & context, const AnalyzedJoin & analyzed_join)
{
    ASTPtr expression_list = analyzed_join.rightKeysList();
    auto syntax_result = SyntaxAnalyzer(context).analyze(expression_list,
                                                         analyzed_join.columnsFromJoinedTable(), analyzed_join.requiredJoinedNames());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActions(true, false);
}

void SelectQueryExpressionAnalyzer::makeTableJoin(const ASTTablesInSelectQueryElement & join_element)
{
    /// Two JOINs are not supported with the same subquery, but different USINGs.
    auto join_hash = join_element.getTreeHash();
    String join_subquery_id = toString(join_hash.first) + "_" + toString(join_hash.second);

    SubqueryForSet & subquery_for_set = subqueries_for_sets[join_subquery_id];

    /// Special case - if table name is specified on the right of JOIN, then the table has the type Join (the previously prepared mapping).
    if (!subquery_for_set.join)
        subquery_for_set.join = tryGetStorageJoin(join_element, context);

    if (!subquery_for_set.join)
    {
        /// Actions which need to be calculated on joined block.
        ExpressionActionsPtr joined_block_actions = createJoinedBlockActions(context, analyzedJoin());

        if (!subquery_for_set.source)
            makeSubqueryForJoin(join_element, joined_block_actions, subquery_for_set);

        /// Test actions on sample block (early error detection)
        Block sample_block = subquery_for_set.renamedSampleBlock();
        joined_block_actions->execute(sample_block);

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_set.join = analyzedJoin().makeHashJoin(sample_block, settings.size_limits_for_join);
        subquery_for_set.joined_block_actions = joined_block_actions;
    }

    syntax->analyzed_join->setHashJoin(subquery_for_set.join);
}

void SelectQueryExpressionAnalyzer::makeSubqueryForJoin(const ASTTablesInSelectQueryElement & join_element,
                                                        const ExpressionActionsPtr & joined_block_actions,
                                                        SubqueryForSet & subquery_for_set) const
{
    /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
        * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
        *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
        * - this function shows the expression JOIN _data1.
        */

    NamesWithAliases required_columns_with_aliases =
        analyzedJoin().getRequiredColumns(joined_block_actions->getSampleBlock(), joined_block_actions->getRequiredColumns());

    Names original_columns;
    for (auto & pr : required_columns_with_aliases)
        original_columns.push_back(pr.first);

    auto interpreter = interpretSubquery(join_element.table_expression, context, subquery_depth, original_columns);

    subquery_for_set.makeSource(interpreter, std::move(required_columns_with_aliases));
}

bool SelectQueryExpressionAnalyzer::appendPrewhere(
    ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->prewhere())
        return false;

    initChain(chain, sourceColumns());
    auto & step = chain.getLastStep();
    getRootActions(select_query->prewhere(), only_types, step.actions);
    String prewhere_column_name = select_query->prewhere()->getColumnName();
    step.required_output.push_back(prewhere_column_name);
    step.can_remove_required_output.push_back(true);

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions = std::make_shared<ExpressionActions>(sourceColumns(), context);
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

        for (const auto & column : sourceColumns())
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

        for (const auto & column : sourceColumns())
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

bool SelectQueryExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->where())
        return false;

    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->where()->getColumnName());
    step.can_remove_required_output = {true};

    getRootActions(select_query->where(), only_types, step.actions);

    return true;
}

bool SelectQueryExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->groupBy())
        return false;

    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    ASTs asts = select_query->groupBy()->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        step.required_output.push_back(asts[i]->getColumnName());
        getRootActions(asts[i], only_types, step.actions);
    }

    return true;
}

void SelectQueryExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();

    for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
    {
        for (size_t j = 0; j < aggregate_descriptions[i].argument_names.size(); ++j)
        {
            step.required_output.push_back(aggregate_descriptions[i].argument_names[j]);
        }
    }

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
        for (auto & argument : node->arguments->children)
            getRootActions(argument, only_types, step.actions);
}

bool SelectQueryExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->having())
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    step.required_output.push_back(select_query->having()->getColumnName());
    getRootActions(select_query->having(), only_types, step.actions);

    return true;
}

void SelectQueryExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->select(), only_types, step.actions);

    for (const auto & child : select_query->select()->children)
        step.required_output.push_back(child->getColumnName());
}

bool SelectQueryExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

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

bool SelectQueryExpressionAnalyzer::appendLimitBy(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->limitBy())
        return false;

    initChain(chain, aggregated_columns);
    ExpressionActionsChain::Step & step = chain.steps.back();

    getRootActions(select_query->limitBy(), only_types, step.actions);

    NameSet aggregated_names;
    for (const auto & column : aggregated_columns)
    {
        step.required_output.push_back(column.name);
        aggregated_names.insert(column.name);
    }

    for (const auto & child : select_query->limitBy()->children)
    {
        auto child_name = child->getColumnName();
        if (!aggregated_names.count(child_name))
            step.required_output.push_back(std::move(child_name));
    }

    return true;
}

void SelectQueryExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    const auto * select_query = getSelectQuery();

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
    initChain(chain, sourceColumns());
    ExpressionActionsChain::Step & step = chain.steps.back();
    getRootActions(expr, only_types, step.actions);
    step.required_output.push_back(expr->getColumnName());
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result)
{
    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(sourceColumns(), context);
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
        for (const auto & column_name_type : sourceColumns())
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

void SelectQueryExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const
{
    for (const auto & name_and_type : aggregation_keys)
        key_names.emplace_back(name_and_type.name);

    aggregates = aggregate_descriptions;
}

}
