#include <Core/Block.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/DumpASTNode.h>

#include <DataTypes/DataTypeNullable.h>
#include <Columns/IColumn.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Set.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/DictionaryReader.h>
#include <Interpreters/Context.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>

#include <Storages/StorageDistributed.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageJoin.h>

#include <DataStreams/copyData.h>

#include <Dictionaries/DictionaryStructure.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/misc.h>

#include <Interpreters/ActionsVisitor.h>

#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>

namespace DB
{

using LogAST = DebugASTLog<false>; /// set to true to enable logs


namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int ILLEGAL_PREWHERE;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
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
            auto name = node.function_base->getName();
            if (name == "ignore")
                return false;
        }
    }
    return true;
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


ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const TreeRewriterResultPtr & syntax_analyzer_result_,
    const Context & context_,
    size_t subquery_depth_,
    bool do_global,
    SubqueriesForSets subqueries_for_sets_)
    : query(query_), context(context_), settings(context.getSettings())
    , subquery_depth(subquery_depth_)
    , syntax(syntax_analyzer_result_)
{
    subqueries_for_sets = std::move(subqueries_for_sets_);

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

    auto temp_actions = std::make_shared<ActionsDAG>(sourceColumns());

    if (select_query)
    {
        NamesAndTypesList array_join_columns;
        columns_after_array_join = sourceColumns();

        bool is_array_join_left;
        if (ASTPtr array_join_expression_list = select_query->arrayJoinExpressionList(is_array_join_left))
        {
            getRootActionsNoMakeSet(array_join_expression_list, true, temp_actions, false);

            auto array_join = addMultipleArrayJoinAction(temp_actions, is_array_join_left);
            auto sample_columns = temp_actions->getResultColumns();
            array_join->prepare(sample_columns);
            temp_actions = std::make_shared<ActionsDAG>(sample_columns);

            NamesAndTypesList new_columns_after_array_join;
            NameSet added_columns;

            for (auto & column : temp_actions->getResultColumns())
            {
                if (syntax->array_join_result_to_source.count(column.name))
                {
                    new_columns_after_array_join.emplace_back(column.name, column.type);
                    added_columns.emplace(column.name);
                }
            }

            for (auto & column : columns_after_array_join)
                if (added_columns.count(column.name) == 0)
                    new_columns_after_array_join.emplace_back(column.name, column.type);

            columns_after_array_join.swap(new_columns_after_array_join);
        }

        columns_after_array_join.insert(columns_after_array_join.end(), array_join_columns.begin(), array_join_columns.end());

        const ASTTablesInSelectQueryElement * join = select_query->join();
        if (join)
        {
            getRootActionsNoMakeSet(analyzedJoin().leftKeysList(), true, temp_actions, false);
            auto sample_columns = temp_actions->getResultColumns();
            analyzedJoin().addJoinedColumnsAndCorrectNullability(sample_columns);
            temp_actions = std::make_shared<ActionsDAG>(sample_columns);
        }

        columns_after_join = columns_after_array_join;
        const auto & added_by_join = analyzedJoin().columnsAddedByJoin();
        columns_after_join.insert(columns_after_join.end(), added_by_join.begin(), added_by_join.end());
    }

    has_aggregation = makeAggregateDescriptions(temp_actions);
    if (select_query && (select_query->groupBy() || select_query->having()))
        has_aggregation = true;

    if (has_aggregation)
    {

        /// Find out aggregation keys.
        if (select_query)
        {
            if (select_query->groupBy())
            {
                NameSet unique_keys;
                ASTs & group_asts = select_query->groupBy()->children;
                for (ssize_t i = 0; i < ssize_t(group_asts.size()); ++i)
                {
                    ssize_t size = group_asts.size();
                    getRootActionsNoMakeSet(group_asts[i], true, temp_actions, false);

                    const auto & column_name = group_asts[i]->getColumnName();
                    const auto & index = temp_actions->getIndex();

                    auto it = index.find(column_name);
                    if (it == index.end())
                        throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                    const auto & node = *it;

                    /// Constant expressions have non-null column pointer at this stage.
                    if (node->column && isColumnConst(*node->column))
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

                    NameAndTypePair key{column_name, node->result_type};

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
                    has_aggregation = select_query->having() || !aggregate_descriptions.empty();
                }
            }
        }
        else
            aggregated_columns = temp_actions->getNamesAndTypesList();

        for (const auto & desc : aggregate_descriptions)
            aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
    }
    else
    {
        aggregated_columns = temp_actions->getNamesAndTypesList();
    }
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables(bool do_global)
{
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

    if (auto set_ptr_from_storage_set = isPlainStorageSetInSubquery(subquery_or_table_name))
    {
        prepared_sets.insert({set_key, set_ptr_from_storage_set});
        return;
    }

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, context, {}, query_options);
    auto stream = interpreter_subquery->execute().getInputStream();

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true, context.getSettingsRef().transform_null_in);
    set->setHeader(stream->getHeader());

    stream->readPrefix();
    while (Block block = stream->read())
    {
        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    set->finishInsert();
    stream->readSuffix();

    prepared_sets[set_key] = std::move(set);
}

SetPtr SelectQueryExpressionAnalyzer::isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name)
{
    const auto * table = subquery_or_table_name->as<ASTIdentifier>();
    if (!table)
        return nullptr;
    auto table_id = context.resolveStorageID(subquery_or_table_name);
    const auto storage = DatabaseCatalog::instance().getTable(table_id, context);
    if (storage->getName() != "Set")
        return nullptr;
    const auto storage_set = std::dynamic_pointer_cast<StorageSet>(storage);
    return storage_set->getSet();
}


/// Performance optimisation for IN() if storage supports it.
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

        if (storage()->mayBenefitFromIndexForIn(left_in_operand, context, metadata_snapshot))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTIdentifier>())
            {
                if (settings.use_index_for_in_with_subqueries)
                    tryMakeSetForIndexFromSubquery(arg);
            }
            else
            {
                auto temp_actions = std::make_shared<ActionsDAG>(columns_after_join);
                getRootActions(left_in_operand, true, temp_actions);

                if (temp_actions->getIndex().contains(left_in_operand->getColumnName()))
                    makeExplicitSet(func, *temp_actions, true, context,
                        settings.size_limits_for_set, prepared_sets);
            }
        }
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(context, settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, false, only_consts, !isRemoteStorage());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsNoMakeSet(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(context, settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, true, only_consts, !isRemoteStorage());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}

void ExpressionAnalyzer::getRootActionsForHaving(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(context, settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, false, only_consts, true);
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


bool ExpressionAnalyzer::makeAggregateDescriptions(ActionsDAGPtr & actions)
{
    for (const ASTFunction * node : aggregates())
    {
        AggregateDescription aggregate;
        getRootActionsNoMakeSet(node->arguments, true, actions);

        aggregate.column_name = node->getColumnName();

        const ASTs & arguments = node->arguments->children;
        aggregate.argument_names.resize(arguments.size());
        DataTypes types(arguments.size());

        const auto & index = actions->getIndex();
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const std::string & name = arguments[i]->getColumnName();

            auto it = index.find(name);
            if (it == index.end())
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown identifier (in aggregate function '{}'): {}", node->name, name);

            types[i] = (*it)->result_type;
            aggregate.argument_names[i] = name;
        }

        AggregateFunctionProperties properties;
        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters, properties);

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

/// "Big" ARRAY JOIN.
ArrayJoinActionPtr ExpressionAnalyzer::addMultipleArrayJoinAction(ActionsDAGPtr & actions, bool array_join_is_left) const
{
    NameSet result_columns;
    for (const auto & result_source : syntax->array_join_result_to_source)
    {
        /// Assign new names to columns, if needed.
        if (result_source.first != result_source.second)
            actions->addAlias(result_source.second, result_source.first);

        /// Make ARRAY JOIN (replace arrays with their insides) for the columns in these new names.
        result_columns.insert(result_source.first);
    }

    return std::make_shared<ArrayJoinAction>(result_columns, array_join_is_left, context);
}

ArrayJoinActionPtr SelectQueryExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, ActionsDAGPtr & before_array_join, bool only_types)
{
    const auto * select_query = getSelectQuery();

    bool is_array_join_left;
    ASTPtr array_join_expression_list = select_query->arrayJoinExpressionList(is_array_join_left);
    if (!array_join_expression_list)
        return nullptr;

    ExpressionActionsChain::Step & step = chain.lastStep(sourceColumns());

    getRootActions(array_join_expression_list, only_types, step.actions());

    auto array_join = addMultipleArrayJoinAction(step.actions(), is_array_join_left);
    before_array_join = chain.getLastActions();

    chain.steps.push_back(std::make_unique<ExpressionActionsChain::ArrayJoinStep>(
            array_join, step.getResultColumns()));

    chain.addStep();

    return array_join;
}

bool SelectQueryExpressionAnalyzer::appendJoinLeftKeys(ExpressionActionsChain & chain, bool only_types)
{
    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);

    getRootActions(analyzedJoin().leftKeysList(), only_types, step.actions());
    return true;
}

JoinPtr SelectQueryExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain)
{
    JoinPtr table_join = makeTableJoin(*syntax->ast_join);

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);

    chain.steps.push_back(std::make_unique<ExpressionActionsChain::JoinStep>(
            syntax->analyzed_join, table_join, step.getResultColumns()));

    chain.addStep();
    return table_join;
}

static JoinPtr tryGetStorageJoin(std::shared_ptr<TableJoin> analyzed_join)
{
    if (auto * table = analyzed_join->joined_storage.get())
        if (auto * storage_join = dynamic_cast<StorageJoin *>(table))
            return storage_join->getJoin(analyzed_join);
    return {};
}

static ExpressionActionsPtr createJoinedBlockActions(const Context & context, const TableJoin & analyzed_join)
{
    ASTPtr expression_list = analyzed_join.rightKeysList();
    auto syntax_result = TreeRewriter(context).analyze(expression_list, analyzed_join.columnsFromJoinedTable());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActions(true, false);
}

static bool allowDictJoin(StoragePtr joined_storage, const Context & context, String & dict_name, String & key_name)
{
    const auto * dict = dynamic_cast<const StorageDictionary *>(joined_storage.get());
    if (!dict)
        return false;

    dict_name = dict->resolvedDictionaryName();
    auto dictionary = context.getExternalDictionariesLoader().getDictionary(dict_name);
    if (!dictionary)
        return false;

    const DictionaryStructure & structure = dictionary->getStructure();
    if (structure.id)
    {
        key_name = structure.id->name;
        return true;
    }
    return false;
}

static std::shared_ptr<IJoin> makeJoin(std::shared_ptr<TableJoin> analyzed_join, const Block & sample_block, const Context & context)
{
    bool allow_merge_join = analyzed_join->allowMergeJoin();

    /// HashJoin with Dictionary optimisation
    String dict_name;
    String key_name;
    if (analyzed_join->joined_storage && allowDictJoin(analyzed_join->joined_storage, context, dict_name, key_name))
    {
        Names original_names;
        NamesAndTypesList result_columns;
        if (analyzed_join->allowDictJoin(key_name, sample_block, original_names, result_columns))
        {
            analyzed_join->dictionary_reader = std::make_shared<DictionaryReader>(dict_name, original_names, result_columns, context);
            return std::make_shared<HashJoin>(analyzed_join, sample_block);
        }
    }

    if (analyzed_join->forceHashJoin() || (analyzed_join->preferMergeJoin() && !allow_merge_join))
        return std::make_shared<HashJoin>(analyzed_join, sample_block);
    else if (analyzed_join->forceMergeJoin() || (analyzed_join->preferMergeJoin() && allow_merge_join))
        return std::make_shared<MergeJoin>(analyzed_join, sample_block);
    return std::make_shared<JoinSwitcher>(analyzed_join, sample_block);
}

JoinPtr SelectQueryExpressionAnalyzer::makeTableJoin(const ASTTablesInSelectQueryElement & join_element)
{
    /// Two JOINs are not supported with the same subquery, but different USINGs.
    auto join_hash = join_element.getTreeHash();
    String join_subquery_id = toString(join_hash.first) + "_" + toString(join_hash.second);

    SubqueryForSet & subquery_for_join = subqueries_for_sets[join_subquery_id];

    /// Use StorageJoin if any.
    if (!subquery_for_join.join)
        subquery_for_join.join = tryGetStorageJoin(syntax->analyzed_join);

    if (!subquery_for_join.join)
    {
        /// Actions which need to be calculated on joined block.
        ExpressionActionsPtr joined_block_actions = createJoinedBlockActions(context, analyzedJoin());

        Names original_right_columns;
        if (!subquery_for_join.source)
        {
            NamesWithAliases required_columns_with_aliases = analyzedJoin().getRequiredColumns(
                joined_block_actions->getSampleBlock(), joined_block_actions->getRequiredColumns());
            for (auto & pr : required_columns_with_aliases)
                original_right_columns.push_back(pr.first);

            /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
                * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
                *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
                * - this function shows the expression JOIN _data1.
                */
            auto interpreter = interpretSubquery(join_element.table_expression, context, original_right_columns, query_options);

            subquery_for_join.makeSource(interpreter, std::move(required_columns_with_aliases));
        }

        /// TODO You do not need to set this up when JOIN is only needed on remote servers.
        subquery_for_join.setJoinActions(joined_block_actions); /// changes subquery_for_join.sample_block inside
        subquery_for_join.join = makeJoin(syntax->analyzed_join, subquery_for_join.sample_block, context);

        /// Do not make subquery for join over dictionary.
        if (syntax->analyzed_join->dictionary_reader)
        {
            JoinPtr join = subquery_for_join.join;
            subqueries_for_sets.erase(join_subquery_id);
            return join;
        }
    }

    return subquery_for_join.join;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendPrewhere(
    ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns)
{
    const auto * select_query = getSelectQuery();
    ActionsDAGPtr prewhere_actions;

    if (!select_query->prewhere())
        return prewhere_actions;

    auto & step = chain.lastStep(sourceColumns());
    getRootActions(select_query->prewhere(), only_types, step.actions());
    String prewhere_column_name = select_query->prewhere()->getColumnName();
    step.required_output.push_back(prewhere_column_name);
    step.can_remove_required_output.push_back(true);

    auto filter_type = (*step.actions()->getIndex().find(prewhere_column_name))->result_type;
    if (!filter_type->canBeUsedInBooleanContext())
        throw Exception("Invalid type for filter in PREWHERE: " + filter_type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions_dag = std::make_shared<ActionsDAG>(sourceColumns());
        getRootActions(select_query->prewhere(), only_types, tmp_actions_dag);
        tmp_actions_dag->removeUnusedActions({prewhere_column_name});
        auto tmp_actions = std::make_shared<ExpressionActions>(tmp_actions_dag);
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

        auto names = step.actions()->getNames();
        NameSet name_set(names.begin(), names.end());

        for (const auto & column : sourceColumns())
            if (required_source_columns.count(column.name) == 0)
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
            if (prewhere_input_names.count(column.name) == 0)
            {
                columns.emplace_back(column.type, column.name);
                unused_source_columns.emplace(column.name);
            }
        }

        chain.steps.emplace_back(std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(
                std::make_shared<ActionsDAG>(std::move(columns))));
        chain.steps.back()->additional_input = std::move(unused_source_columns);
        chain.getLastActions();
        chain.addStep();
    }

    return prewhere_actions;
}

void SelectQueryExpressionAnalyzer::appendPreliminaryFilter(ExpressionActionsChain & chain, ActionsDAGPtr actions_dag, String column_name)
{
    ExpressionActionsChain::Step & step = chain.lastStep(sourceColumns());

    // FIXME: assert(filter_info);
    auto * expression_step = typeid_cast<ExpressionActionsChain::ExpressionActionsStep *>(&step);
    expression_step->actions_dag = std::move(actions_dag);
    step.required_output.push_back(std::move(column_name));
    step.can_remove_required_output = {true};

    chain.addStep();
}

bool SelectQueryExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    if (!select_query->where())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_join);

    getRootActions(select_query->where(), only_types, step.actions());

    auto where_column_name = select_query->where()->getColumnName();
    step.required_output.push_back(where_column_name);
    step.can_remove_required_output = {true};

    auto filter_type = (*step.actions()->getIndex().find(where_column_name))->result_type;
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
    for (const auto & ast : asts)
    {
        step.required_output.emplace_back(ast->getColumnName());
        getRootActions(ast, only_types, step.actions());
    }

    if (optimize_aggregation_in_order)
    {
        for (auto & child : asts)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            group_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(actions_dag));
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
            step.required_output.emplace_back(name);

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
            getRootActions(argument, only_types, step.actions());
}

bool SelectQueryExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getAggregatingQuery();

    if (!select_query->having())
        return false;

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActionsForHaving(select_query->having(), only_types, step.actions());
    step.required_output.push_back(select_query->having()->getColumnName());

    return true;
}

void SelectQueryExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    getRootActions(select_query->select(), only_types, step.actions());

    for (const auto & child : select_query->select()->children)
        step.required_output.push_back(child->getColumnName());
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

    getRootActions(select_query->orderBy(), only_types, step.actions());

    bool with_fill = false;
    NameSet order_by_keys;
    for (auto & child : select_query->orderBy()->children)
    {
        const auto * ast = child->as<ASTOrderByElement>();
        if (!ast || ast->children.empty())
            throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
        ASTPtr order_expression = ast->children.at(0);
        step.required_output.push_back(order_expression->getColumnName());

        if (ast->with_fill)
            with_fill = true;
    }

    if (optimize_read_in_order)
    {
        for (auto & child : select_query->orderBy()->children)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            order_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(actions_dag));
        }
    }

    NameSet non_constant_inputs;
    if (with_fill)
    {
        for (const auto & column : step.getResultColumns())
            if (!order_by_keys.count(column.name))
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

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendProjectResult(ExpressionActionsChain & chain) const
{
    const auto * select_query = getSelectQuery();

    ExpressionActionsChain::Step & step = chain.lastStep(aggregated_columns);

    NamesWithAliases result_columns;

    ASTs asts = select_query->select()->children;
    for (const auto & ast : asts)
    {
        String result_name = ast->getAliasOrColumnName();
        if (required_result_columns.empty() || required_result_columns.count(result_name))
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
            step.required_output.push_back(result_columns.back().second);
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
    step.required_output.push_back(expr->getColumnName());
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
        getRootActions(ast, false, actions_dag);
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
        /// We will not delete the original columns.
        for (const auto & column_name_type : sourceColumns())
            result_names.push_back(column_name_type.name);
    }

    actions_dag->removeUnusedActions(result_names);
    return actions_dag;
}

ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result)
{
    return std::make_shared<ExpressionActions>(getActionsDAG(add_aliases, project_result));
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
    auto actions = std::make_shared<ActionsDAG>(NamesAndTypesList());

    getRootActions(query, true, actions, true);
    return std::make_shared<ExpressionActions>(actions);
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::simpleSelectActions()
{
    ExpressionActionsChain new_chain(context);
    appendSelect(new_chain, false);
    return new_chain.getLastActions();
}

ExpressionAnalysisResult::ExpressionAnalysisResult(
        SelectQueryExpressionAnalyzer & query_analyzer,
        const StorageMetadataPtr & metadata_snapshot,
        bool first_stage_,
        bool second_stage_,
        bool only_types,
        const FilterInfoPtr & filter_info_,
        const Block & source_header)
    : first_stage(first_stage_)
    , second_stage(second_stage_)
    , need_aggregate(query_analyzer.hasAggregation())
{
    /// first_stage: Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
    /// second_stage: Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.

    /** First we compose a chain of actions and remember the necessary steps from it.
        *  Regardless of from_stage and to_stage, we will compose a complete sequence of actions to perform optimization and
        *  throw out unnecessary columns based on the entire query. In unnecessary parts of the query, we will not execute subqueries.
        */

    const ASTSelectQuery & query = *query_analyzer.getSelectQuery();
    const Context & context = query_analyzer.context;
    const Settings & settings = context.getSettingsRef();
    const ConstStoragePtr & storage = query_analyzer.storage();

    bool finalized = false;
    size_t where_step_num = 0;

    auto finalize_chain = [&](ExpressionActionsChain & chain)
    {
        chain.finalize();

        if (!finalized)
        {
            finalize(chain, where_step_num);
            finalized = true;
        }

        chain.clear();
    };

    if (storage)
    {
        query_analyzer.makeSetsForIndex(query.where());
        query_analyzer.makeSetsForIndex(query.prewhere());
    }

    {
        ExpressionActionsChain chain(context);
        Names additional_required_columns_after_prewhere;

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

        if (storage && filter_info_)
        {
            filter_info = filter_info_;
            query_analyzer.appendPreliminaryFilter(chain, filter_info->actions_dag, filter_info->column_name);
        }

        if (auto actions = query_analyzer.appendPrewhere(chain, !first_stage, additional_required_columns_after_prewhere))
        {
            prewhere_info = std::make_shared<PrewhereDAGInfo>(actions, query.prewhere()->getColumnName());

            if (allowEarlyConstantFolding(*prewhere_info->prewhere_actions, settings))
            {
                Block before_prewhere_sample = source_header;
                if (sanitizeBlock(before_prewhere_sample))
                {
                    ExpressionActions(prewhere_info->prewhere_actions).execute(before_prewhere_sample);
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
            join = query_analyzer.appendJoin(chain);
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
                    ExpressionActions(before_where).execute(before_where_sample);
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
                    context.getSettingsRef().optimize_aggregation_in_order
                    && storage && query.groupBy();

            query_analyzer.appendGroupBy(chain, only_types || !first_stage, optimize_aggregation_in_order, group_by_elements_actions);
            query_analyzer.appendAggregateFunctionsArguments(chain, only_types || !first_stage);
            before_aggregation = chain.getLastActions();

            finalize_chain(chain);

            if (query_analyzer.appendHaving(chain, only_types || !second_stage))
            {
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
            && storage && query.orderBy()
            && !query_analyzer.hasAggregation()
            && !query.final()
            && join_allow_read_in_order;

        /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
        query_analyzer.appendSelect(chain, only_types || (need_aggregate ? !second_stage : !first_stage));
        selected_columns = chain.getLastStep().required_output;
        has_order_by = query.orderBy() != nullptr;
        before_order_and_select = query_analyzer.appendOrderBy(
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

void ExpressionAnalysisResult::finalize(const ExpressionActionsChain & chain, size_t where_step_num)
{
    if (hasPrewhere())
    {
        const ExpressionActionsChain::Step & step = *chain.steps.at(0);
        prewhere_info->remove_prewhere_column = step.can_remove_required_output.at(0);

        NameSet columns_to_remove;
        for (size_t i = 1; i < step.required_output.size(); ++i)
        {
            if (step.can_remove_required_output[i])
                columns_to_remove.insert(step.required_output[i]);
        }

        if (!columns_to_remove.empty())
        {
            auto columns = prewhere_info->prewhere_actions->getResultColumns();

            auto remove_actions = std::make_shared<ActionsDAG>();
            for (const auto & column : columns)
            {
                if (columns_to_remove.count(column.name))
                {
                    remove_actions->addInput(column);
                    remove_actions->removeColumn(column.name);
                }
            }

            prewhere_info->remove_columns_actions = std::move(remove_actions);
        }

        columns_to_remove_after_prewhere = std::move(columns_to_remove);
    }
    else if (hasFilter())
    {
        /// Can't have prewhere and filter set simultaneously
        filter_info->do_remove_column = chain.steps.at(0)->can_remove_required_output.at(0);
    }
    if (hasWhere())
        remove_where_filter = chain.steps.at(where_step_num)->can_remove_required_output.at(0);
}

void ExpressionAnalysisResult::removeExtraColumns() const
{
    if (hasFilter())
        filter_info->actions_dag->projectInput();
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
        check_actions(prewhere_info->alias_actions);
        check_actions(prewhere_info->remove_columns_actions);
    }
}

}
