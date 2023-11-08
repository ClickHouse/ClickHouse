#include <Formats/FormatFactory.h>
#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Coordinator.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentBuilder.h>
#include <QueryCoordination/Interpreters/InterpreterSelectQueryCoordination.h>
#include <QueryCoordination/Interpreters/ReplaceDistributedTableNameVisitor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include "QueryCoordination/Optimizer/CostBasedOptimizer.h"


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
}

InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
    : InterpreterSelectQueryCoordination(query_ptr_, Context::createCopy(context_), options_)
{
}

InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
    const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_)
    : query_ptr(query_ptr_), context(context_), options(options_)
{
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY && !options_.is_subquery)
    {
        setIncompatibleSettings();

        // Only propagate WITH elements to subqueries if we're not a subquery
        if (!options.is_subquery)
        {
            if (context->getSettingsRef().enable_global_with_statement)
                ApplyWithAliasVisitor().visit(query_ptr);
            ApplyWithSubqueryVisitor().visit(query_ptr);
        }

        ReplaceDistributedTableNameVisitor visitor(context);
        visitor.visit(query_ptr);

        if (visitor.has_table_function)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support table function query");

        if (visitor.has_local_table && visitor.has_distributed_table)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support distributed table and local table mix query");

        String cluster_name;
        if (visitor.has_distributed_table)
        {
            cluster_name = visitor.clusters[0]->getName();
            /// remote() cluster_name is empty
            if (cluster_name.empty())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support remote function query");
            for (size_t i = 1; i < visitor.clusters.size(); ++i)
                if (cluster_name != visitor.clusters[i]->getName())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query");

            if (context->addQueryCoordinationMetaInfo(cluster_name, visitor.storages, visitor.sharding_keys))
                context->setDistributedForQueryCoord(true);
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query"); /// maybe union query
        }

        if (visitor.has_local_table)
            context->setDistributedForQueryCoord(false);
    }
    else
    {
        context->setDistributedForQueryCoord(true);
    }

    query_coordination_enabled = context->isDistributedForQueryCoord();
}

void InterpreterSelectQueryCoordination::setIncompatibleSettings()
{
    context->getSettings().use_index_for_in_with_subqueries = 0;
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true);
    ast_format_settings.hilite = false;
    ast_format_settings.always_quote_identifiers = true;
    ast->format(ast_format_settings);
    return buf.str();
}

void InterpreterSelectQueryCoordination::buildQueryPlanIfNeeded()
{
    if (plan.isInitialized())
        return;
    if (context->getSettingsRef().allow_experimental_analyzer)
        plan = InterpreterSelectQueryAnalyzer(query_ptr, context, options).extractQueryPlan();
    else if (query_ptr->as<ASTSelectQuery>())
        InterpreterSelectQuery(query_ptr, context, options).buildQueryPlan(plan);
    else
        InterpreterSelectWithUnionQuery(query_ptr, context, options).buildQueryPlan(plan);
}

void InterpreterSelectQueryCoordination::optimize()
{
    if (!plan.isInitialized())
        return;

    /// Optimized by RBO rules
    plan.optimize(QueryPlanOptimizationSettings::fromContext(context));

    /// Optimized by CBO optimizer
    if (query_coordination_enabled)
    {
        CostBasedOptimizer optimizer;
        plan = optimizer.optimize(std::move(plan), context);
    }
}

void InterpreterSelectQueryCoordination::buildFragments()
{
    if (query_coordination_enabled)
    {
        FragmentBuilder builder(plan, context);
        FragmentPtr root_fragment = builder.build();

        std::queue<FragmentPtr> queue;
        queue.push(root_fragment);

        while (!queue.empty())
        {
            auto fragment = queue.front();
            queue.pop();
            fragments.emplace_back(fragment);
            for (const auto & child : fragment->getChildren())
                queue.push(child);
        }
    }
}

void InterpreterSelectQueryCoordination::explainFragment(WriteBufferFromOwnString & buf, const Fragment::ExplainFragmentOptions & options_)
{
    if (!query_coordination_enabled)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "EXPLAIN FRAGMENT but query coordination is not enabled.");

    buildQueryPlanIfNeeded();
    optimize();
    buildFragments();

    if (fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "EXPLAIN FRAGMENT but there is no fragments.");

    fragments.front()->dump(buf, options_);
}

void InterpreterSelectQueryCoordination::explain(
    WriteBufferFromOwnString & buf, const QueryPlan::ExplainPlanOptions & options_, bool json, bool optimize_)
{
    buildQueryPlanIfNeeded();

    if (optimize_)
        optimize();

    if (json)
    {
        /// Add extra layers to make plan look more like from postgres.
        auto plan_map = std::make_unique<JSONBuilder::JSONMap>();

        plan_map->add("Plan", plan.explainPlan(options_));
        auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
        plan_array->add(std::move(plan_map));

        auto format_settings = getFormatSettings(getContext());
        format_settings.json.quote_64bit_integers = false;

        JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
        JSONBuilder::FormatContext format_context{.out = buf};

        plan_array->format(json_format_settings, format_context);
    }
    else
    {
        plan.explainPlan(buf, options_);
    }
}


BlockIO InterpreterSelectQueryCoordination::execute()
{
    BlockIO res;

    buildQueryPlanIfNeeded();
    optimize();

    if (query_coordination_enabled)
    {
        buildFragments();

        /// save fragments wait for be scheduled
        res.query_coord_state.fragments = fragments;

        /// schedule fragments
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            Coordinator coord(fragments, context, formattedAST(query_ptr));
            coord.schedulePrepareDistributedPipelines();

            /// local already be scheduled
            res.query_coord_state.pipelines = std::move(coord.pipelines);
            res.query_coord_state.remote_host_connection = coord.getRemoteHostConnection();
            res.pipeline = res.query_coord_state.pipelines.detachRootPipeline();

            /// TODO quota only use to root pipeline?
            if (!options.ignore_quota)
                res.pipeline.setQuota(context->getQuota());
        }
    }
    else
    {
        LOG_INFO(&Poco::Logger::get("InterpreterSelectQueryCoordination"), "disable query_coordination");

        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        if (!options.ignore_quota)
            res.pipeline.setQuota(context->getQuota());
    }

    return res;
}

}
