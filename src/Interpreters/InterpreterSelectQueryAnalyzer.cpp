#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/QueryLog.h>

#include <Core/ProtocolDefines.h>
#include "config_version.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

ASTPtr normalizeAndValidateQuery(const ASTPtr & query)
{
    if (query->as<ASTSelectWithUnionQuery>() || query->as<ASTSelectQuery>())
    {
        return query;
    }
    else if (auto * subquery = query->as<ASTSubquery>())
    {
        return subquery->children[0];
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected ASTSelectWithUnionQuery or ASTSelectQuery. Actual {}",
            query->formatForErrorMessage());
    }
}

QueryTreeNodePtr buildQueryTreeAndRunPasses(const ASTPtr & query, const SelectQueryOptions & select_query_options, const ContextPtr & context)
{
    auto query_tree = buildQueryTree(query, context);

    QueryTreePassManager query_tree_pass_manager(context);
    addQueryTreePasses(query_tree_pass_manager);

    if (select_query_options.ignore_ast_optimizations)
        query_tree_pass_manager.run(query_tree, 1 /*up_to_pass_index*/);
    else
        query_tree_pass_manager.run(query_tree);

    return query_tree;
}

}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_)
    : query(normalizeAndValidateQuery(query_))
    , context(Context::createCopy(context_))
    , select_query_options(select_query_options_)
    , query_tree(buildQueryTreeAndRunPasses(query, select_query_options, context))
    , planner(query_tree, select_query_options)
{
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const QueryTreeNodePtr & query_tree_,
    const ContextPtr & context_,
    const SelectQueryOptions & select_query_options_)
    : query(query_tree_->toAST())
    , context(Context::createCopy(context_))
    , select_query_options(select_query_options_)
    , query_tree(query_tree_)
    , planner(query_tree, select_query_options)
{
}

Block InterpreterSelectQueryAnalyzer::getSampleBlock()
{
    planner.buildQueryPlanIfNeeded();
    return planner.getQueryPlan().getCurrentDataStream().header;
}

BlockIO InterpreterSelectQueryAnalyzer::execute()
{
    planner.buildQueryPlanIfNeeded();
    auto & query_plan = planner.getQueryPlan();

    QueryPlanOptimizationSettings optimization_settings;
    BuildQueryPipelineSettings build_pipeline_settings;
    auto pipeline_builder = query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings);

    BlockIO result;
    result.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

    if (!select_query_options.ignore_quota && (select_query_options.to_stage == QueryProcessingStage::Complete))
        result.pipeline.setQuota(context->getQuota());

    return result;
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

    QueryPlanOptimizationSettings optimization_settings;
    BuildQueryPipelineSettings build_pipeline_settings;

    return std::move(*query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings));
}

void InterpreterSelectQueryAnalyzer::addStorageLimits(const StorageLimitsList & storage_limits)
{
    planner.addStorageLimits(storage_limits);
}

void InterpreterSelectQueryAnalyzer::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}

}
