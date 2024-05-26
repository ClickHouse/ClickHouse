#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>

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

#include <Interpreters/Cache/QueryCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

ASTPtr normalizeAndValidateQuery(const ASTPtr & query, const Names & column_names)
{
    ASTPtr result_query;

    if (query->as<ASTSelectWithUnionQuery>() || query->as<ASTSelectQuery>())
        result_query = query;
    else if (auto * subquery = query->as<ASTSubquery>())
        result_query = subquery->children[0];
    else
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected ASTSelectWithUnionQuery or ASTSelectQuery. Actual {}",
            query->formatForErrorMessage());

    if (column_names.empty())
        return result_query;

    /// The initial query the VIEW references to is wrapped here with another SELECT query to allow reading only necessary columns.
    auto select_query = std::make_shared<ASTSelectQuery>();

    auto result_table_expression_ast = std::make_shared<ASTTableExpression>();
    result_table_expression_ast->children.push_back(std::make_shared<ASTSubquery>(std::move(result_query)));
    result_table_expression_ast->subquery = result_table_expression_ast->children.back();

    auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_query_element_ast->children.push_back(std::move(result_table_expression_ast));
    tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
    tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

    auto projection_expression_list_ast = std::make_shared<ASTExpressionList>();
    projection_expression_list_ast->children.reserve(column_names.size());

    for (const auto & column_name : column_names)
        projection_expression_list_ast->children.push_back(std::make_shared<ASTIdentifier>(column_name));

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

QueryTreeNodePtr buildQueryTreeAndRunPasses(const ASTPtr & query,
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
        || context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        query_tree_pass_manager.runOnlyResolve(query_tree);
    else
        query_tree_pass_manager.run(query_tree);

    if (storage)
        replaceStorageInQueryTree(query_tree, context, storage);

    return query_tree;
}

}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_,
    const Names & column_names)
    : query(normalizeAndValidateQuery(query_, column_names))
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, nullptr /*storage*/))
    , planner(query_tree, select_query_options)
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const ContextPtr & context_,
    const StoragePtr & storage_,
    const SelectQueryOptions & select_query_options_,
    const Names & column_names)
    : query(normalizeAndValidateQuery(query_, column_names))
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, storage_))
    , planner(query_tree, select_query_options)
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const QueryTreeNodePtr & query_tree_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_)
    : query(query_tree_->toAST())
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(query_tree_)
    , planner(query_tree_, select_query_options)
{
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock(const ASTPtr & query,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryAnalyzer interpreter(query, context, select_query_options_copy);

    return interpreter.getSampleBlock();
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryAnalyzer interpreter(query_tree, context, select_query_options_copy);

    return interpreter.getSampleBlock();
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getQueryPlan().getCurrentDataStream().header;
}

namespace
{

String queryStringFromAst(const ASTPtr & ast)
{
    /// Reconstruct the query string from the AST (needed for pretty-printing)
    WriteBufferFromOwnString buf;
    formatAST(*ast, buf, /*hilite*/ false, /*one_line*/ true, /*show_secrets*/ false);
    return buf.str();
}

bool tryReadResultFromQueryCache(
    const QueryCachePtr & query_cache,
    const QueryTreeNodePtr & query_tree, const ASTPtr & query,
    const ContextPtr & context, const Settings & settings,
    BlockIO & result)
{
    if (!settings.enable_reads_from_query_cache)
        return false;

    QueryCache::Key key(query_tree->toAST(), context->getCurrentDatabase(), context->getSettingsRef(), context->getUserID(), context->getCurrentRoles(), queryStringFromAst(query));
    QueryCache::Reader reader = query_cache->createReader(key);
    if (!reader.hasCacheEntryForKey())
        return false;

    /// Replace the existing pipeline by a pipeline which reads the result from the query cache
    QueryPipeline new_pipeline;
    new_pipeline.readFromQueryCache(reader.getSource(), reader.getSourceTotals(), reader.getSourceExtremes());
    result.pipeline = std::move(new_pipeline);
    return true;
}

void writeResultIntoQueryCache(
    const QueryCachePtr & query_cache,
    const QueryTreeNodePtr & query_tree, const ASTPtr & query,
    const ContextPtr & context, const Settings & settings,
    BlockIO & result)
{
    /// If we really write on the query cache depends on a few things ...
    if (!settings.enable_writes_to_query_cache)
        return;

    if (!QueryCache::astIsEligibleForCaching(query, context, settings))
        return;

    QueryCache::Key key(
        query_tree->toAST(), context->getCurrentDatabase(), context->getSettingsRef(),
        result.pipeline.getHeader(),
        context->getUserID(), context->getCurrentRoles(),
        settings.query_cache_share_between_users,
        std::chrono::system_clock::now() + std::chrono::seconds(settings.query_cache_ttl),
        settings.query_cache_compress_entries,
        queryStringFromAst(query));

    size_t num_query_runs = settings.query_cache_min_query_runs ? query_cache->recordQueryRun(key) : 1; /// avoid locking the mutex in recordQueryRun()
    if (num_query_runs <= settings.query_cache_min_query_runs)
        return;

    /// Add a processor on top of the pipeline which forwards the result to the query cache
    auto writer = std::make_shared<QueryCache::Writer>(query_cache->createWriter(
                     key, std::chrono::milliseconds(settings.query_cache_min_query_duration.totalMilliseconds()),
                     settings.query_cache_squash_partial_results, settings.max_block_size,
                     settings.query_cache_max_size_in_bytes, settings.query_cache_max_entries));
    result.pipeline.writeResultIntoQueryCache(writer);
}

}

BlockIO InterpreterSelectQueryAnalyzer::execute()
{
    auto pipeline_builder = buildQueryPipeline();

    BlockIO result;
    result.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

    if (!select_query_options.ignore_quota && select_query_options.to_stage == QueryProcessingStage::Complete)
        result.pipeline.setQuota(context->getQuota());

    /// Can we write/read the result into/from the query cache? Requires to run the query with 'SETTINGS use_query_cache = 1'
    QueryCachePtr query_cache = context->getQueryCache();
    const Settings & settings = context->getSettingsRef();
    bool use_query_cache = settings.use_query_cache && (query_cache != nullptr) && !select_query_options.is_internal;
    if (use_query_cache)
    {
        /// If the query runs with "use_query_cache = 1", we first probe if the query cache already contains the query result (if yes:
        /// return result from cache). If doesn't, we execute the query normally and write the result into the query cache. Both steps use a
        /// hash of the AST, the current database and the settings as cache key. Unfortunately, the settings are in some places internally
        /// modified between steps 1 and 2 (= during query execution) - this is silly but hard to forbid. As a result, the hashes no longer
        /// match and the cache is rendered ineffective. Therefore make a copy of the settings and use it for steps 1 and 2.
        /// std::optional<Settings> settings_copy;
        /// if (can_use_query_cache)
        ///     settings_copy = settings;
        /// TODO

        bool read_from_query_cache = tryReadResultFromQueryCache(query_cache, query_tree, query, context, settings, result);
        if (read_from_query_cache)
            return result;
        writeResultIntoQueryCache(query_cache, query_tree, query, context, settings, result);
    }

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

    auto optimization_settings = QueryPlanOptimizationSettings::fromContext(context);
    auto build_pipeline_settings = BuildQueryPipelineSettings::fromContext(context);

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
