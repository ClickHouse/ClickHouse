#include <cstddef>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

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
#include "Planner/PlannerContext.h"
#include "Planner/PlannerExpressionAnalysis.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_concurrency_control;
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

namespace
{

struct StorageSubstituionOptions
{
    const StoragePtr & storage = nullptr;
    const TableNodePtr & table = nullptr;
};

QueryTreeNodePtr buildQueryTreeAndRunPasses(
    const ASTPtr & query,
    const SelectQueryOptions & select_query_options,
    const ContextPtr & context,
    StorageSubstituionOptions storage_options = {}
)
{
    auto query_tree = buildQueryTree(query, context);
    if (storage_options.table)
    {
        auto * query_node = query_tree->as<QueryNode>();
        if (!query_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Invalid query provided during projection calculation: Received not a QueryNode");
        auto * identifier_node = query_node->getJoinTree()->as<IdentifierNode>();
        if (identifier_node == nullptr || identifier_node->getIdentifier().getFullName() != "system.one")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Invalid query provided during projection calculation: Invalid FROM clause");

        query_node->getJoinTree() = storage_options.table;
    }

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager, select_query_options.only_analyze);

    /// We should not apply any query tree level optimizations on shards
    /// because it can lead to a changed header.
    if (select_query_options.ignore_ast_optimizations
        || context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        query_tree_pass_manager.runOnlyResolve(query_tree);
    else
        query_tree_pass_manager.run(query_tree);

    if (storage_options.storage)
        replaceStorageInQueryTree(query_tree, context, storage_options.storage);

    if (storage_options.table)
    {
        auto * query_node = query_tree->as<QueryNode>();

        std::unordered_set<size_t> used_projection_columns_indexes;

        QueryTreeNodePtrWithHashSet projection_set;
        const auto & projection_list = query_node->getProjection().getNodes();
        for (size_t i = 0; i < projection_list.size(); ++i)
        {
            auto [_, inserted] = projection_set.emplace(projection_list[i]);
            if (inserted)
                used_projection_columns_indexes.emplace(i);
        }

        query_node->removeUnusedProjectionColumns(used_projection_columns_indexes);
    }

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
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context))
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
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, { .storage = storage_ }))
    , planner(query_tree, select_query_options)
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const ContextPtr & context_,
    TableNodePtr table_,
    const SelectQueryOptions & select_query_options_
)
    : query(normalizeAndValidateQuery(query_, {}))
    , context(buildContext(context_, select_query_options_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context, { .table = table_ }))
    , planner(query_tree, select_query_options, /*qualify_column_names*/false)
{}

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

std::pair<Block, PlannerContextPtr> InterpreterSelectQueryAnalyzer::getSampleBlockAndPlannerContext(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    auto select_query_options_copy = select_query_options;
    select_query_options_copy.only_analyze = true;
    InterpreterSelectQueryAnalyzer interpreter(query_tree, context, select_query_options_copy);

    return interpreter.getSampleBlockAndPlannerContext();
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context,
    const SelectQueryOptions & select_query_options)
{
    return getSampleBlockAndPlannerContext(query_tree, context, select_query_options).first;
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getQueryPlan().getCurrentHeader();
}

std::pair<Block, PlannerContextPtr> InterpreterSelectQueryAnalyzer::getSampleBlockAndPlannerContext()
{
    planner.buildQueryPlanIfNeeded();
    return {planner.getQueryPlan().getCurrentHeader(), planner.getPlannerContext()};
}

const Names & InterpreterSelectQueryAnalyzer::getRequiredColumns()
{
    planner.buildQueryPlanIfNeeded();
    auto const & table_expression_data_map = planner.getPlannerContext()->getTableExpressionNodeToData();
    if (table_expression_data_map.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Expected to have only 1 table expression registered, but got: {}",
        table_expression_data_map.size());
    return table_expression_data_map.begin()->second.getSelectedColumnsNames();
}

const PlannerExpressionsAnalysisResult & InterpreterSelectQueryAnalyzer::getExpressionAnalysisResult()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getExpressionAnalysisResult();
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
