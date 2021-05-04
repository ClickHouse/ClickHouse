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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
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

#include <Processors/QueryPlan/ExpressionStep.h>

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

#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/GlobalSubqueriesVisitor.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/join_common.h>
#include <Interpreters/misc.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
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
{}

ExpressionAnalyzer::~ExpressionAnalyzer() = default;

ExpressionAnalyzer::ExpressionAnalyzer(
    const ASTPtr & query_,
    const TreeRewriterResultPtr & syntax_analyzer_result_,
    ContextPtr context_,
    size_t subquery_depth_,
    bool do_global,
    SubqueriesForSets subqueries_for_sets_)
    : WithContext(context_)
    , query(query_), settings(getContext()->getSettings())
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
            analyzedJoin().addJoinedColumnsAndCorrectTypes(sample_columns);
            temp_actions = std::make_shared<ActionsDAG>(sample_columns);
        }

        columns_after_join = columns_after_array_join;
        analyzedJoin().addJoinedColumnsAndCorrectTypes(columns_after_join, false);
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
                    const auto * node = temp_actions->tryFindInIndex(column_name);
                    if (!node)
                        throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

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
        GlobalSubqueriesVisitor::Data subqueries_data(getContext(), subquery_depth, isRemoteStorage(),
                                                   external_tables, subqueries_for_sets, has_global_subqueries);
        GlobalSubqueriesVisitor(subqueries_data).visit(query);
    }
}


void ExpressionAnalyzer::tryMakeSetForIndexFromSubquery(const ASTPtr & subquery_or_table_name, const SelectQueryOptions & query_options)
{
    auto set_key = PreparedSetKey::forSubquery(*subquery_or_table_name);

    if (prepared_sets.count(set_key))
        return; /// Already prepared.

    if (auto set_ptr_from_storage_set = isPlainStorageSetInSubquery(subquery_or_table_name))
    {
        prepared_sets.insert({set_key, set_ptr_from_storage_set});
        return;
    }

    auto interpreter_subquery = interpretSubquery(subquery_or_table_name, getContext(), {}, query_options);
    auto io = interpreter_subquery->execute();
    PullingAsyncPipelineExecutor executor(io.pipeline);

    SetPtr set = std::make_shared<Set>(settings.size_limits_for_set, true, getContext()->getSettingsRef().transform_null_in);
    set->setHeader(executor.getHeader());

    Block block;
    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;

        /// If the limits have been exceeded, give up and let the default subquery processing actions take place.
        if (!set->insertFromBlock(block))
            return;
    }

    set->finishInsert();

    prepared_sets[set_key] = std::move(set);
}

SetPtr ExpressionAnalyzer::isPlainStorageSetInSubquery(const ASTPtr & subquery_or_table_name)
{
    const auto * table = subquery_or_table_name->as<ASTIdentifier>();
    if (!table)
        return nullptr;
    auto table_id = getContext()->resolveStorageID(subquery_or_table_name);
    const auto storage = DatabaseCatalog::instance().getTable(table_id, getContext());
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
    if (func && functionIsInOrGlobalInOperator(func->name))
    {
        const IAST & args = *func->arguments;
        const ASTPtr & left_in_operand = args.children.at(0);

        if (storage()->mayBenefitFromIndexForIn(left_in_operand, getContext(), metadata_snapshot))
        {
            const ASTPtr & arg = args.children.at(1);
            if (arg->as<ASTSubquery>() || arg->as<ASTIdentifier>())
            {
                if (settings.use_index_for_in_with_subqueries)
                    tryMakeSetForIndexFromSubquery(arg, query_options);
            }
            else
            {
                auto temp_actions = std::make_shared<ActionsDAG>(columns_after_join);
                getRootActions(left_in_operand, true, temp_actions);

                if (temp_actions->tryFindInIndex(left_in_operand->getColumnName()))
                    makeExplicitSet(func, *temp_actions, true, getContext(),
                        settings.size_limits_for_set, prepared_sets);
            }
        }
    }
}


void ExpressionAnalyzer::getRootActions(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(getContext(), settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, false, only_consts, !isRemoteStorage());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}


void ExpressionAnalyzer::getRootActionsNoMakeSet(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(getContext(), settings.size_limits_for_set, subquery_depth,
                                   sourceColumns(), std::move(actions), prepared_sets, subqueries_for_sets,
                                   no_subqueries, true, only_consts, !isRemoteStorage());
    ActionsVisitor(visitor_data, log.stream()).visit(ast);
    actions = visitor_data.getActions();
}

void ExpressionAnalyzer::getRootActionsForHaving(const ASTPtr & ast, bool no_subqueries, ActionsDAGPtr & actions, bool only_consts)
{
    LogAST log;
    ActionsVisitor::Data visitor_data(getContext(), settings.size_limits_for_set, subquery_depth,
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
        if (node->arguments)
            getRootActionsNoMakeSet(node->arguments, true, actions);

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
        aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters) : Array();
        aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters, properties);

        aggregate_descriptions.push_back(aggregate);
    }

    return !aggregates().empty();
}

void makeWindowDescriptionFromAST(WindowDescription & desc, const IAST * ast)
{
    const auto & definition = ast->as<const ASTWindowDefinition &>();

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
        }
    }

    desc.full_sort_description = desc.partition_by;
    desc.full_sort_description.insert(desc.full_sort_description.end(),
        desc.order_by.begin(), desc.order_by.end());

    if (definition.frame.type != WindowFrame::FrameType::Rows
        && definition.frame.type != WindowFrame::FrameType::Range)
    {
        std::string name = definition.frame.type == WindowFrame::FrameType::Rows
            ? "ROWS"
            : definition.frame.type == WindowFrame::FrameType::Groups
                ? "GROUPS" : "RANGE";

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Window frame '{}' is not implemented (while processing '{}')",
            name, ast->formatForErrorMessage());
    }

    desc.frame = definition.frame;
}

void ExpressionAnalyzer::makeWindowDescriptions(ActionsDAGPtr actions)
{
    // Convenient to check here because at least we have the Context.
    if (!syntax->window_function_asts.empty() &&
        !getContext()->getSettingsRef().allow_experimental_window_functions)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "The support for window functions is experimental and will change"
            " in backwards-incompatible ways in the future releases. Set"
            " allow_experimental_window_functions = 1 to enable it."
            " While processing '{}'",
            syntax->window_function_asts[0]->formatForErrorMessage());
    }

    // Window definitions from the WINDOW clause
    const auto * select_query = query->as<ASTSelectQuery>();
    if (select_query && select_query->window())
    {
        for (const auto & ptr : select_query->window()->children)
        {
            const auto & elem = ptr->as<const ASTWindowListElement &>();
            WindowDescription desc;
            desc.window_name = elem.name;
            makeWindowDescriptionFromAST(desc, elem.definition.get());

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
                    window_function.function_node->parameters)
                : Array();

        // Requiring a constant reference to a shared pointer to non-const AST
        // doesn't really look sane, but the visitor does indeed require it.
        // Hence we clone the node (not very sane either, I know).
        getRootActionsNoMakeSet(window_function.function_node->clone(),
            true, actions);

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
            makeWindowDescriptionFromAST(desc, &definition);

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
    const ColumnsWithTypeAndName & left_sample_columns = chain.getLastStep().getResultColumns();
    JoinPtr table_join = makeTableJoin(*syntax->ast_join, left_sample_columns);

    if (syntax->analyzed_join->needConvert())
    {
        chain.steps.push_back(std::make_unique<ExpressionActionsChain::ExpressionActionsStep>(syntax->analyzed_join->leftConvertingActions()));
        chain.addStep();
    }

    ExpressionActionsChain::Step & step = chain.lastStep(columns_after_array_join);
    chain.steps.push_back(std::make_unique<ExpressionActionsChain::JoinStep>(syntax->analyzed_join, table_join, step.getResultColumns()));
    chain.addStep();
    return table_join;
}

static JoinPtr tryGetStorageJoin(std::shared_ptr<TableJoin> analyzed_join)
{
    if (auto * table = analyzed_join->joined_storage.get())
        if (auto * storage_join = dynamic_cast<StorageJoin *>(table))
            return storage_join->getJoinLocked(analyzed_join);
    return {};
}

static ActionsDAGPtr createJoinedBlockActions(ContextPtr context, const TableJoin & analyzed_join)
{
    ASTPtr expression_list = analyzed_join.rightKeysList();
    auto syntax_result = TreeRewriter(context).analyze(expression_list, analyzed_join.columnsFromJoinedTable());
    return ExpressionAnalyzer(expression_list, syntax_result, context).getActionsDAG(true, false);
}

static bool allowDictJoin(StoragePtr joined_storage, ContextPtr context, String & dict_name, String & key_name)
{
    if (!joined_storage->isDictionary())
        return false;

    StorageDictionary & storage_dictionary = static_cast<StorageDictionary &>(*joined_storage);
    dict_name = storage_dictionary.getDictionaryName();
    auto dictionary = context->getExternalDictionariesLoader().getDictionary(dict_name, context);
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

static std::shared_ptr<IJoin> makeJoin(std::shared_ptr<TableJoin> analyzed_join, const Block & sample_block, ContextPtr context)
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

JoinPtr SelectQueryExpressionAnalyzer::makeTableJoin(
    const ASTTablesInSelectQueryElement & join_element, const ColumnsWithTypeAndName & left_sample_columns)
{
    /// Two JOINs are not supported with the same subquery, but different USINGs.

    if (joined_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table join was already created for query");

    /// Use StorageJoin if any.
    JoinPtr join = tryGetStorageJoin(syntax->analyzed_join);

    if (!join)
    {
        /// Actions which need to be calculated on joined block.
        auto joined_block_actions = createJoinedBlockActions(getContext(), analyzedJoin());

        Names original_right_columns;

        NamesWithAliases required_columns_with_aliases = analyzedJoin().getRequiredColumns(
            Block(joined_block_actions->getResultColumns()), joined_block_actions->getRequiredColumns().getNames());
        for (auto & pr : required_columns_with_aliases)
            original_right_columns.push_back(pr.first);

        /** For GLOBAL JOINs (in the case, for example, of the push method for executing GLOBAL subqueries), the following occurs
            * - in the addExternalStorage function, the JOIN (SELECT ...) subquery is replaced with JOIN _data1,
            *   in the subquery_for_set object this subquery is exposed as source and the temporary table _data1 as the `table`.
            * - this function shows the expression JOIN _data1.
            */
        auto interpreter = interpretSubquery(join_element.table_expression, getContext(), original_right_columns, query_options);

        {
            joined_plan = std::make_unique<QueryPlan>();
            interpreter->buildQueryPlan(*joined_plan);

            auto sample_block = interpreter->getSampleBlock();

            auto rename_dag = std::make_unique<ActionsDAG>(sample_block.getColumnsWithTypeAndName());
            for (const auto & name_with_alias : required_columns_with_aliases)
            {
                if (sample_block.has(name_with_alias.first))
                {
                    auto pos = sample_block.getPositionByName(name_with_alias.first);
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

        const ColumnsWithTypeAndName & right_sample_columns = joined_plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
        bool need_convert = syntax->analyzed_join->applyJoinKeyConvert(left_sample_columns, right_sample_columns);
        if (need_convert)
        {
            auto converting_step = std::make_unique<ExpressionStep>(joined_plan->getCurrentDataStream(), syntax->analyzed_join->rightConvertingActions());
            converting_step->setStepDescription("Convert joined columns");
            joined_plan->addStep(std::move(converting_step));
        }

        join = makeJoin(syntax->analyzed_join, joined_plan->getCurrentDataStream().header, getContext());

        /// Do not make subquery for join over dictionary.
        if (syntax->analyzed_join->dictionary_reader)
            joined_plan.reset();
    }
    else
        syntax->analyzed_join->applyJoinKeyConvert(left_sample_columns, {});

    return join;
}

ActionsDAGPtr SelectQueryExpressionAnalyzer::appendPrewhere(
    ExpressionActionsChain & chain, bool only_types, const Names & additional_required_columns)
{
    const auto * select_query = getSelectQuery();
    ActionsDAGPtr prewhere_actions;

    if (!select_query->prewhere())
        return prewhere_actions;

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

    {
        /// Remove unused source_columns from prewhere actions.
        auto tmp_actions_dag = std::make_shared<ActionsDAG>(sourceColumns());
        getRootActions(select_query->prewhere(), only_types, tmp_actions_dag);
        tmp_actions_dag->removeUnusedActions(NameSet{prewhere_column_name});
        auto tmp_actions = std::make_shared<ExpressionActions>(tmp_actions_dag, ExpressionActionsSettings::fromContext(getContext()));
        auto required_columns = tmp_actions->getRequiredColumns();
        NameSet required_source_columns(required_columns.begin(), required_columns.end());
        required_source_columns.insert(first_action_names.begin(), first_action_names.end());

        /// Add required columns to required output in order not to remove them after prewhere execution.
        /// TODO: add sampling and final execution to common chain.
        for (const auto & column : additional_required_columns)
        {
            if (required_source_columns.count(column))
                step.addRequiredOutput(column);
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
    step.addRequiredOutput(column_name);

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
    for (const auto & ast : asts)
    {
        step.addRequiredOutput(ast->getColumnName());
        getRootActions(ast, only_types, step.actions());
    }

    if (optimize_aggregation_in_order)
    {
        for (auto & child : asts)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            group_by_elements_actions.emplace_back(
                std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext())));
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
        getRootActionsNoMakeSet(getSelectQuery()->window(),
            true /* no_subqueries */, step.actions());
    }

    for (const auto & [_, w] : window_descriptions)
    {
        for (const auto & f : w.window_functions)
        {
            // (1b) Actions for function arguments, and also the inline window
            // definitions (1a).
            // Requiring a constant reference to a shared pointer to non-const AST
            // doesn't really look sane, but the visitor does indeed require it.
            getRootActionsNoMakeSet(f.function_node->clone(),
                true /* no_subqueries */, step.actions());

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
    {
        if (const auto * function = typeid_cast<const ASTFunction *>(child.get());
            function
            && function->is_window_function)
        {
            // Skip window function columns here -- they are calculated after
            // other SELECT expressions by a special step.
            continue;
        }

        step.addRequiredOutput(child->getColumnName());
    }
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
        step.addRequiredOutput(order_expression->getColumnName());

        if (ast->with_fill)
            with_fill = true;
    }

    if (optimize_read_in_order)
    {
        for (auto & child : select_query->orderBy()->children)
        {
            auto actions_dag = std::make_shared<ActionsDAG>(columns_after_join);
            getRootActions(child, only_types, actions_dag);
            order_by_elements_actions.emplace_back(
                std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext())));
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
        step.addRequiredOutput(column.name);
        aggregated_names.insert(column.name);
    }

    for (const auto & child : select_query->limitBy()->children)
    {
        auto child_name = child->getColumnName();
        if (!aggregated_names.count(child_name))
            step.addRequiredOutput(std::move(child_name));
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
        NameSet name_set(result_names.begin(), result_names.end());
        /// We will not delete the original columns.
        for (const auto & column_name_type : sourceColumns())
        {
            if (name_set.count(column_name_type.name) == 0)
            {
                result_names.push_back(column_name_type.name);
                name_set.insert(column_name_type.name);
            }
        }

        actions_dag->removeUnusedActions(name_set);
    }

    return actions_dag;
}

ExpressionActionsPtr ExpressionAnalyzer::getActions(bool add_aliases, bool project_result)
{
    return std::make_shared<ExpressionActions>(
        getActionsDAG(add_aliases, project_result), ExpressionActionsSettings::fromContext(getContext()));
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions(const ColumnsWithTypeAndName & constant_inputs)
{
    auto actions = std::make_shared<ActionsDAG>(constant_inputs);

    getRootActions(query, true, actions, true);
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
        const Block & source_header)
    : first_stage(first_stage_)
    , second_stage(second_stage_)
    , need_aggregate(query_analyzer.hasAggregation())
    , has_window(query_analyzer.hasWindow())
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

    bool finalized = false;
    size_t where_step_num = 0;

    auto finalize_chain = [&](ExpressionActionsChain & chain)
    {
        chain.finalize();

        if (!finalized)
        {
            finalize(chain, where_step_num, query);
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
            filter_info->do_remove_column = true;
        }

        if (auto actions = query_analyzer.appendPrewhere(chain, !first_stage, additional_required_columns_after_prewhere))
        {
            prewhere_info = std::make_shared<PrewhereDAGInfo>(actions, query.prewhere()->getColumnName());

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
            join = query_analyzer.appendJoin(chain);
            converting_join_columns = query_analyzer.analyzedJoin().leftConvertingActions();
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
            && storage
            && query.orderBy()
            && !query_analyzer.hasAggregation()
            && !query_analyzer.hasWindow()
            && !query.final()
            && join_allow_read_in_order;

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

            auto & step = chain.lastStep(query_analyzer.columns_after_window);

            // The output of this expression chain is the result of
            // SELECT (before "final projection" i.e. renaming the columns), so
            // we have to mark the expressions that are required in the output,
            // again. We did it for the previous expression chain ("select w/o
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

void ExpressionAnalysisResult::finalize(const ExpressionActionsChain & chain, size_t where_step_num, const ASTSelectQuery & query)
{
    size_t next_step_i = 0;

    if (hasPrewhere())
    {
        const ExpressionActionsChain::Step & step = *chain.steps.at(next_step_i++);
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
    }

    if (hasWhere())
    {
        auto where_column_name = query.where()->getColumnName();
        remove_where_filter = chain.steps.at(where_step_num)->required_output.find(where_column_name)->second;
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
        check_actions(prewhere_info->alias_actions);
        check_actions(prewhere_info->remove_columns_actions);
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
        for (size_t i = 0; i < selected_columns.size(); i++)
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
