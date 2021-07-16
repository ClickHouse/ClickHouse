#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
}

namespace ClusterProxy
{

ContextMutablePtr updateSettingsForCluster(const Cluster & cluster, ContextPtr context, const Settings & settings, Poco::Logger * log)
{
    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// If "secret" (in remote_servers) is not in use,
    /// user on the shard is not the same as the user on the initiator,
    /// hence per-user limits should not be applied.
    if (cluster.getSecret().empty())
    {
        /// Does not matter on remote servers, because queries are sent under different user.
        new_settings.max_concurrent_queries_for_user = 0;
        new_settings.max_memory_usage_for_user = 0;

        /// Set as unchanged to avoid sending to remote server.
        new_settings.max_concurrent_queries_for_user.changed = false;
        new_settings.max_memory_usage_for_user.changed = false;
    }

    if (settings.force_optimize_skip_unused_shards_nesting && settings.force_optimize_skip_unused_shards)
    {
        if (new_settings.force_optimize_skip_unused_shards_nesting == 1)
        {
            new_settings.force_optimize_skip_unused_shards = false;
            new_settings.force_optimize_skip_unused_shards.changed = false;

            if (log)
                LOG_TRACE(log, "Disabling force_optimize_skip_unused_shards for nested queries (force_optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings.force_optimize_skip_unused_shards_nesting.value;
            new_settings.force_optimize_skip_unused_shards_nesting.changed = true;

            if (log)
                LOG_TRACE(log, "force_optimize_skip_unused_shards_nesting is now {}", new_settings.force_optimize_skip_unused_shards_nesting);
        }
    }

    if (settings.optimize_skip_unused_shards_nesting && settings.optimize_skip_unused_shards)
    {
        if (new_settings.optimize_skip_unused_shards_nesting == 1)
        {
            new_settings.optimize_skip_unused_shards = false;
            new_settings.optimize_skip_unused_shards.changed = false;

            if (log)
                LOG_TRACE(log, "Disabling optimize_skip_unused_shards for nested queries (optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings.optimize_skip_unused_shards_nesting.value;
            new_settings.optimize_skip_unused_shards_nesting.changed = true;

            if (log)
                LOG_TRACE(log, "optimize_skip_unused_shards_nesting is now {}", new_settings.optimize_skip_unused_shards_nesting);
        }
    }

    if (settings.offset)
    {
        new_settings.offset = 0;
        new_settings.offset.changed = false;
    }
    if (settings.limit)
    {
        new_settings.limit = 0;
        new_settings.limit.changed = false;
    }

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    return new_context;
}

void executeQuery(
    QueryPlan & query_plan,
    IStreamFactory & stream_factory, Poco::Logger * log,
    const ASTPtr & query_ast, ContextPtr context, const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const ClusterPtr & not_optimized_cluster)
{
    assert(log);

    const Settings & settings = context->getSettingsRef();

    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth > settings.max_distributed_depth)
        throw Exception("Maximum distributed depth exceeded", ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH);

    std::vector<QueryPlanPtr> plans;
    Pipes remote_pipes;
    Pipes delayed_pipes;

    auto new_context = updateSettingsForCluster(*query_info.getCluster(), context, settings, log);

    new_context->getClientInfo().distributed_depth += 1;

    ThrottlerPtr user_level_throttler;
    if (auto * process_list_element = context->getProcessListElement())
        user_level_throttler = process_list_element->getUserNetworkThrottler();

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings.max_network_bandwidth || settings.max_network_bytes)
    {
        throttler = std::make_shared<Throttler>(
                settings.max_network_bandwidth,
                settings.max_network_bytes,
                "Limit for bytes to send or receive over network exceeded.",
                user_level_throttler);
    }
    else
        throttler = user_level_throttler;

    size_t shards = query_info.getCluster()->getShardCount();
    for (const auto & shard_info : query_info.getCluster()->getShardsInfo())
    {
        ASTPtr query_ast_for_shard;
        if (query_info.optimized_cluster && settings.optimize_skip_unused_shards_rewrite_in && shards > 1)
        {
            query_ast_for_shard = query_ast->clone();

            OptimizeShardingKeyRewriteInVisitor::Data visitor_data{
                sharding_key_expr,
                sharding_key_expr->getSampleBlock().getByPosition(0).type,
                sharding_key_column_name,
                shard_info,
                not_optimized_cluster->getSlotToShard(),
            };
            OptimizeShardingKeyRewriteInVisitor visitor(visitor_data);
            visitor.visit(query_ast_for_shard);
        }
        else
            query_ast_for_shard = query_ast;

        stream_factory.createForShard(shard_info,
            query_ast_for_shard,
            new_context, throttler, query_info, plans,
            remote_pipes, delayed_pipes, log);
    }

    if (!remote_pipes.empty())
    {
        auto plan = std::make_unique<QueryPlan>();
        auto read_from_remote = std::make_unique<ReadFromPreparedSource>(Pipe::unitePipes(std::move(remote_pipes)));
        read_from_remote->setStepDescription("Read from remote replica");
        plan->addStep(std::move(read_from_remote));
        plans.emplace_back(std::move(plan));
    }

    if (!delayed_pipes.empty())
    {
        auto plan = std::make_unique<QueryPlan>();
        auto read_from_remote = std::make_unique<ReadFromPreparedSource>(Pipe::unitePipes(std::move(delayed_pipes)));
        read_from_remote->setStepDescription("Read from delayed local replica");
        plan->addStep(std::move(read_from_remote));
        plans.emplace_back(std::move(plan));
    }

    if (plans.empty())
        return;

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

}

}
