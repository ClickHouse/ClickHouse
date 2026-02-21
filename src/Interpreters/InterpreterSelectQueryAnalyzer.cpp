#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>

#include <DataTypes/DataTypesNumber.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/IStorage.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <Interpreters/Context.h>
#include <Interpreters/QueryLog.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace Setting
{
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsString cluster_for_parallel_replicas;
}

namespace
{

ASTPtr createIdentifierFromColumnName(const String & column_name)
{
    Tokens tokens(column_name.data(), column_name.data() + column_name.size(), DBMS_DEFAULT_MAX_QUERY_SIZE);
    IParser::Pos pos(tokens, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    ASTPtr res;
    Expected expected;
    ParserCompoundIdentifier().parse(pos, res, expected);
    if (!res || getIdentifierName(res) != column_name)
        return make_intrusive<ASTIdentifier>(column_name);
    return res;
}

ASTPtr normalizeAndValidateQuery(const ASTPtr & query, const Names & column_names)
{
    ASTPtr result_query;

    if (query->as<ASTSelectWithUnionQuery>() || query->as<ASTSelectQuery>())
        result_query = query;
    else if (auto * subquery = query->as<ASTSubquery>())
        result_query = subquery->children[0];
    else
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected ASTSelectWithUnionQuery, ASTSelectQuery or ASTSubquery. Actual {} ({})",
            query->formatForErrorMessage(), query->getID());

    if (column_names.empty())
        return result_query;

    /// The initial query the VIEW references to is wrapped here with another SELECT query to allow reading only necessary columns.
    auto select_query = make_intrusive<ASTSelectQuery>();

    auto result_table_expression_ast = make_intrusive<ASTTableExpression>();
    result_table_expression_ast->children.push_back(make_intrusive<ASTSubquery>(std::move(result_query)));
    result_table_expression_ast->subquery = result_table_expression_ast->children.back();

    auto tables_in_select_query_element_ast = make_intrusive<ASTTablesInSelectQueryElement>();
    tables_in_select_query_element_ast->children.push_back(std::move(result_table_expression_ast));
    tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

    ASTPtr tables_in_select_query_ast = make_intrusive<ASTTablesInSelectQuery>();
    tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

    auto projection_expression_list_ast = make_intrusive<ASTExpressionList>();
    projection_expression_list_ast->children.reserve(column_names.size());

    for (const auto & column_name : column_names)
        projection_expression_list_ast->children.push_back(createIdentifierFromColumnName(column_name));

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection_expression_list_ast));

    return select_query;
}

ContextMutablePtr buildContext(const ContextPtr & context, const SelectQueryOptions & select_query_options)
{
    auto result_context = Context::createCopy(context);

    if (select_query_options.shard_num)
        result_context->addSpecialScalar(
            "_shard_num",
            Block{{DataTypeUInt32().createColumnConst(1, *select_query_options.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}});
    if (select_query_options.shard_count)
        result_context->addSpecialScalar(
            "_shard_count",
            Block{{DataTypeUInt32().createColumnConst(1, *select_query_options.shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});

    return result_context;
}

template <typename... Args>
QueryPlanPtr buildQueryPlanForAutomaticParallelReplicas(
    const ASTPtr & ast, const ContextMutablePtr & ctx, const SelectQueryOptions & select_options, Args &&... interpreter_args)
{
    const auto & logger = getLogger("InterpreterSelectQueryAnalyzer");
    if (!ctx->getSettingsRef()[Setting::parallel_replicas_local_plan])
    {
        LOG_TRACE(logger, "Setting 'parallel_replicas_local_plan' is disabled. Skipping building query plan with parallel replicas.");
        return QueryPlanPtr{};
    }
    if (ctx->getSettingsRef()[Setting::cluster_for_parallel_replicas].value.empty())
    {
        LOG_DEBUG(logger, "Cluster for parallel replicas is not set, can't build plan with parallel replicas");
        return QueryPlanPtr{};
    }
    /// If the query is executed by remote*/cluster* function, the following attempt to build a plan with parallel replicas may result in exceptions
    if (ctx->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        return QueryPlanPtr{};
    ctx->setSetting("enable_parallel_replicas", true);
    // We don't want to analyze primaty key at all, see `query_plan_optimize_primary_key` below.
    ctx->setSetting("force_primary_key", false);
    InterpreterSelectQueryAnalyzer interpreter(ast, ctx, select_options, std::forward<Args>(interpreter_args)...);
    auto plan = std::move(interpreter).extractQueryPlan();
    auto optimization_settings = QueryPlanOptimizationSettings(ctx);
    // We should build sets and create `CreatingSetsStep` only in the original plan. The automatic parallel replicas optimization happens before building sets,
    // so even if we decide to use the plan with parallel replicas, we will substitute it in place of the original plan and then build sets.
    optimization_settings.build_sets = false;
    // If the parallel replicas plan will be chosen, the index analysis result will be reused from the single-replica plan.
    optimization_settings.query_plan_optimize_primary_key = false;
    // Depends on PK optimizations that we don't perform here
    optimization_settings.optimize_projection = false;
    optimization_settings.force_use_projection = false;
    optimization_settings.force_projection_name.clear();
    plan.optimize(optimization_settings);
    return std::make_unique<QueryPlan>(std::move(plan));
}
}

void replaceStorageInQueryTree(QueryTreeNodePtr & query_tree, const ContextPtr & context, const StoragePtr & storage)
{
    auto nodes = extractAllTableReferences(query_tree);
    IQueryTreeNode::ReplacementMap replacement_map;

    for (auto & node : nodes)
    {
        auto & table_node = node->as<TableNode &>();

        /// Don't replace storage if table name differs
        if (table_node.getStorageID().getFullNameNotQuoted() != storage->getStorageID().getFullNameNotQuoted())
            continue;

        auto replacement_table_expression = std::make_shared<TableNode>(storage, context);
        replacement_table_expression->setAlias(node->getAlias());

        if (auto table_expression_modifiers = table_node.getTableExpressionModifiers())
            replacement_table_expression->setTableExpressionModifiers(*table_expression_modifiers);

        replacement_map.emplace(node.get(), std::move(replacement_table_expression));
    }
    query_tree = query_tree->cloneAndReplace(replacement_map);
}

static QueryTreeNodePtr buildQueryTreeAndRunPasses(const ASTPtr & query,
    const SelectQueryOptions & select_query_options,
    const ContextPtr & context,
    const StoragePtr & storage)
{
    auto query_tree = buildQueryTree(query, context);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager, select_query_options.only_analyze);

    /// We should not apply any query tree level optimizations on shards
    /// because it can lead to a changed header.
    if (select_query_options.ignore_ast_optimizations
        || select_query_options.is_create_view
        || context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        query_tree_pass_manager.runOnlyResolve(query_tree);
    else
        query_tree_pass_manager.run(query_tree);

    if (storage)
        replaceStorageInQueryTree(query_tree, context, storage);

    return query_tree;
}


InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_, const ContextPtr & context_, const SelectQueryOptions & select_query_options_, const Names & column_names)
    : query(normalizeAndValidateQuery(query_, column_names))
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, nullptr /*storage*/))
    , planner(query_tree, select_query_options)
    , query_plan_with_parallel_replicas_builder(
          [ast = query_->clone(), ctx = Context::createCopy(context_), select_options = select_query_options_, column_names]()
          { return buildQueryPlanForAutomaticParallelReplicas(ast, ctx, select_options, column_names); })
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_,
    const StoragePtr & storage_,
    const Names & column_names)
    : query(normalizeAndValidateQuery(query_, column_names))
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, storage_))
    , planner(query_tree, select_query_options)
    , query_plan_with_parallel_replicas_builder(
          [ast = query_->clone(),
           ctx = Context::createCopy(context_),
           storage = storage_,
           select_options = select_query_options_,
           column_names]() { return buildQueryPlanForAutomaticParallelReplicas(ast, ctx, select_options, storage, column_names); })
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const QueryTreeNodePtr & query_tree_, const ContextPtr & context_, const SelectQueryOptions & select_query_options_)
    : query(query_tree_->toAST())
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(query_tree_)
    , planner(query_tree_, select_query_options)
    , query_plan_with_parallel_replicas_builder(
          [tree = query_tree_->clone(), ctx = Context::createCopy(context_), select_options = select_query_options_]()
          { return buildQueryPlanForAutomaticParallelReplicas(tree->toAST(), ctx, select_options); })
{
}

SharedHeader InterpreterSelectQueryAnalyzer::getSampleBlock(const ASTPtr & query,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryAnalyzer interpreter(query, context, select_query_options_copy);

    return interpreter.getSampleBlock();
}

std::pair<SharedHeader, PlannerContextPtr> InterpreterSelectQueryAnalyzer::getSampleBlockAndPlannerContext(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryAnalyzer interpreter(query_tree, context, select_query_options_copy);

    return interpreter.getSampleBlockAndPlannerContext();
}

SharedHeader InterpreterSelectQueryAnalyzer::getSampleBlock(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    return getSampleBlockAndPlannerContext(query_tree, context, select_query_options).first;
}

SharedHeader InterpreterSelectQueryAnalyzer::getSampleBlock()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getQueryPlan().getCurrentHeader();
}

std::pair<SharedHeader, PlannerContextPtr> InterpreterSelectQueryAnalyzer::getSampleBlockAndPlannerContext()
{
    planner.buildQueryPlanIfNeeded();
    return {planner.getQueryPlan().getCurrentHeader(), planner.getPlannerContext()};
}

BlockIO InterpreterSelectQueryAnalyzer::execute()
{
    auto pipeline_builder = buildQueryPipeline();

    BlockIO result;
    result.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    if (!select_query_options.ignore_quota && select_query_options.to_stage == QueryProcessingStage::Complete)
        result.pipeline.setQuota(context->getQuota());

    return result;
}

QueryPlan & InterpreterSelectQueryAnalyzer::getQueryPlan()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getQueryPlan();
}

QueryPlan && InterpreterSelectQueryAnalyzer::extractQueryPlan() &&
{
    planner.buildQueryPlanIfNeeded();
    return std::move(planner).extractQueryPlan();
}

QueryPipelineBuilder InterpreterSelectQueryAnalyzer::buildQueryPipeline()
{
    planner.buildQueryPlanIfNeeded();
    auto & query_plan = planner.getQueryPlan();

    QueryPlanOptimizationSettings optimization_settings(context);
    optimization_settings.query_plan_with_parallel_replicas_builder = query_plan_with_parallel_replicas_builder;

    BuildQueryPipelineSettings build_pipeline_settings(context);

    query_plan.setConcurrencyControl(context->getSettingsRef()[Setting::use_concurrency_control]);

    return std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));
}

void InterpreterSelectQueryAnalyzer::addStorageLimits(const StorageLimitsList & storage_limits)
{
    planner.addStorageLimits(storage_limits);
}

void InterpreterSelectQueryAnalyzer::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr /*context*/) const
{
    for (const auto & used_row_policy : planner.getUsedRowPolicies())
        elem.used_row_policies.emplace(used_row_policy);
}

void registerInterpreterSelectQueryAnalyzer(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectQueryAnalyzer>(args.query, args.context, args.options);
    };
    factory.registerInterpreter("InterpreterSelectQueryAnalyzer", create_fn);
}

}
