#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Coordinator.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentBuilder.h>
#include <QueryCoordination/Interpreters/InterpreterSelectQueryCoordination.h>
#include <QueryCoordination/Interpreters/RewriteDistributedTableVisitor.h>
#include <QueryCoordination/Optimizer/Optimizer.h>
#include <QueryCoordination/Optimizer/StepTree.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
    : query_ptr(query_ptr_), context(Context::createCopy(context_)), options(options_)
{
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY && !options_.is_subquery)
    {
        RewriteDistributedTableVisitor visitor(context);
        visitor.visit(query_ptr);

        if (visitor.has_local_table && visitor.has_distributed_table)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support distributed table and local table mix query");

        String cluster_name;
        if (visitor.has_distributed_table)
        {
            cluster_name = visitor.clusters[0]->getName();
            for (const auto & cluster : visitor.clusters)
            {
                if (cluster_name != cluster->getName())
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query");
                }
            }

            std::vector<StorageID> storages;
            for (const auto & visitor_storage : visitor.storages)
            {
                storages.emplace_back(visitor_storage->getStorageID());
            }

            if (context->addQueryCoordinationMetaInfo(cluster_name, storages, visitor.sharding_keys))
                context->setDistributed(true);
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not support cross cluster query"); /// maybe union query
        }

        if (visitor.has_local_table)
            context->setDistributed(false);
    }
    else
    {
        context->setDistributed(true);
    }
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

BlockIO InterpreterSelectQueryCoordination::execute()
{
    BlockIO res;

    QueryPlan query_plan;

    if (context->getSettingsRef().allow_experimental_analyzer)
        query_plan = InterpreterSelectQueryAnalyzer(query_ptr, context, options).extractQueryPlan();
    else
        InterpreterSelectWithUnionQuery(query_ptr, context, options).buildQueryPlan(query_plan);

    query_plan.optimize(QueryPlanOptimizationSettings::fromContext(context));

    if (context->isDistributed())
    {
        Optimizer optimizer;
        StepTree step_tree = optimizer.optimize(std::move(query_plan), context);

        FragmentBuilder builder(step_tree, context);
        FragmentPtr root_fragment = builder.build();

        FragmentPtrs fragments;
        std::queue<FragmentPtr> queue;
        queue.push(root_fragment);

        while (!queue.empty())
        {
            auto fragment = queue.front();
            queue.pop();
            fragments.emplace_back(fragment);
            for (const auto & child : fragment->getChildren())
            {
                queue.push(child);
            }
        }

        WriteBufferFromOwnString buffer;
        fragments.front()->dump(buffer);
        LOG_INFO(&Poco::Logger::get("InterpreterSelectQueryCoordination"), "Fragment dump: {}", buffer.str());

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
        auto builder = query_plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context),
            BuildQueryPipelineSettings::fromContext(context));

        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        if (!options.ignore_quota)
            res.pipeline.setQuota(context->getQuota());
    }

    return res;
}

}
