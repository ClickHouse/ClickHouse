#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/SelectQueryInfo.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int LOGICAL_ERROR;
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
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory, Poco::Logger * log,
    const ASTPtr & query_ast, ContextPtr context, const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const ClusterPtr & not_optimized_cluster)
{
    const Settings & settings = context->getSettingsRef();

    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
        throw Exception("Maximum distributed depth exceeded", ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH);

    std::vector<QueryPlanPtr> plans;
    SelectStreamFactory::Shards remote_shards;

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
            query_ast_for_shard, main_table, table_func_ptr,
            new_context, plans, remote_shards, shards);
    }

    if (!remote_shards.empty())
    {
        Scalars scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
        scalars.emplace(
            "_shard_count", Block{{DataTypeUInt32().createColumnConst(1, shards), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
        auto external_tables = context->getExternalTables();

        auto plan = std::make_unique<QueryPlan>();
        auto read_from_remote = std::make_unique<ReadFromRemote>(
            std::move(remote_shards),
            header,
            processed_stage,
            main_table,
            table_func_ptr,
            new_context,
            throttler,
            std::move(scalars),
            std::move(external_tables),
            log,
            shards,
            query_info.storage_limits);

        read_from_remote->setStepDescription("Read from remote replica");
        plan->addStep(std::move(read_from_remote));
        plan->addInterpreterContext(new_context);
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


void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    const ASTPtr & query_ast, ContextPtr context, const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const ClusterPtr & not_optimized_cluster)
{
    const Settings & settings = context->getSettingsRef();

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


    std::vector<QueryPlanPtr> plans;
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

        auto shard_plans = stream_factory.createForShardWithParallelReplicas(shard_info,
            query_ast_for_shard, main_table, table_func_ptr, throttler, context, shards, query_info.storage_limits);

        if (!shard_plans.local_plan && !shard_plans.remote_plan)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No plans were generated for reading from shard. This is a bug");

        if (shard_plans.local_plan)
            plans.emplace_back(std::move(shard_plans.local_plan));

        if (shard_plans.remote_plan)
            plans.emplace_back(std::move(shard_plans.remote_plan));
    }

    if (plans.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No plans were generated for reading from Distributed. This is a bug");

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (const auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

}

}
