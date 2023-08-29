#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/ResizeProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageReplicatedMergeTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int LOGICAL_ERROR;
}

namespace ClusterProxy
{

ContextMutablePtr updateSettingsForCluster(bool interserver_mode,
    ContextPtr context,
    const Settings & settings,
    const StorageID & main_table,
    const SelectQueryInfo * query_info,
    Poco::Logger * log)
{
    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// If "secret" (in remote_servers) is not in use,
    /// user on the shard is not the same as the user on the initiator,
    /// hence per-user limits should not be applied.
    if (!interserver_mode)
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

    /// Setting additional_table_filters may be applied to Distributed table.
    /// In case if query is executed up to WithMergableState on remote shard, it is impossible to filter on initiator.
    /// We need to propagate the setting, but change the table name from distributed to source.
    ///
    /// Here we don't try to analyze setting again. In case if query_info->additional_filter_ast is not empty, some filter was applied.
    /// It's just easier to add this filter for a source table.
    if (query_info && query_info->additional_filter_ast)
    {
        Tuple tuple;
        tuple.push_back(main_table.getShortName());
        tuple.push_back(queryToString(query_info->additional_filter_ast));
        new_settings.additional_table_filters.value.push_back(std::move(tuple));
    }

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    return new_context;
}

static ThrottlerPtr getThrottler(const ContextPtr & context)
{
    const Settings & settings = context->getSettingsRef();

    ThrottlerPtr user_level_throttler;
    if (auto process_list_element = context->getProcessListElement())
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

    return throttler;
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
    const ClusterPtr & not_optimized_cluster,
    AdditionalShardFilterGenerator shard_filter_generator)
{
    const Settings & settings = context->getSettingsRef();

    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth >= settings.max_distributed_depth)
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    std::vector<QueryPlanPtr> plans;
    SelectStreamFactory::Shards remote_shards;

    auto new_context = updateSettingsForCluster(!query_info.getCluster()->getSecret().empty(), context, settings, main_table, &query_info, log);
    new_context->increaseDistributedDepth();

    size_t shards = query_info.getCluster()->getShardCount();
    for (const auto & shard_info : query_info.getCluster()->getShardsInfo())
    {
        ASTPtr query_ast_for_shard = query_ast->clone();
        if (sharding_key_expr && query_info.optimized_cluster && settings.optimize_skip_unused_shards_rewrite_in && shards > 1)
        {
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

        if (shard_filter_generator)
        {
            auto shard_filter = shard_filter_generator(shard_info.shard_num);
            if (shard_filter)
            {
                auto & select_query = query_ast_for_shard->as<ASTSelectQuery &>();

                auto where_expression = select_query.where();
                if (where_expression)
                    shard_filter = makeASTFunction("and", where_expression, shard_filter);

                select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(shard_filter));
            }
        }

        stream_factory.createForShard(shard_info,
            query_ast_for_shard, main_table, table_func_ptr,
            new_context, plans, remote_shards, static_cast<UInt32>(shards));
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
            getThrottler(context),
            std::move(scalars),
            std::move(external_tables),
            log,
            shards,
            query_info.storage_limits,
            query_info.getCluster()->getName());

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
    const ASTPtr & query_ast,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ClusterPtr & not_optimized_cluster)
{
    const auto & settings = context->getSettingsRef();
    auto new_context = Context::createCopy(context);
    auto scalars = new_context->hasQueryContext() ? new_context->getQueryContext()->getScalars() : Scalars{};

    UInt64 shard_num = 0; /// shard_num is 1-based, so 0 - no shard specified
    const auto it = scalars.find("_shard_num");
    if (it != scalars.end())
    {
        const Block & block = it->second;
        const auto & column = block.safeGetByPosition(0).column;
        shard_num = column->getUInt(0);
    }

    size_t all_replicas_count = 0;
    ClusterPtr new_cluster;
    /// if got valid shard_num from query initiator, then parallel replicas scope is the specified shard
    /// shards are numbered in order of appearance in the cluster config
    if (shard_num > 0)
    {
        const auto shard_count = not_optimized_cluster->getShardCount();
        if (shard_num > shard_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Shard number is greater than shard count: shard_num={} shard_count={} cluster={}",
                shard_num,
                shard_count,
                not_optimized_cluster->getName());

        chassert(shard_count == not_optimized_cluster->getShardsAddresses().size());

        LOG_DEBUG(&Poco::Logger::get("executeQueryWithParallelReplicas"), "Parallel replicas query in shard scope: shard_num={} cluster={}",
                  shard_num, not_optimized_cluster->getName());

        const auto shard_replicas_num = not_optimized_cluster->getShardsAddresses()[shard_num - 1].size();
        all_replicas_count = std::min(static_cast<size_t>(settings.max_parallel_replicas), shard_replicas_num);

        /// shard_num is 1-based, but getClusterWithSingleShard expects 0-based index
        new_cluster = not_optimized_cluster->getClusterWithSingleShard(shard_num - 1);
    }
    else
    {
        new_cluster = not_optimized_cluster->getClusterWithReplicasAsShards(settings);
        all_replicas_count = std::min(static_cast<size_t>(settings.max_parallel_replicas), new_cluster->getShardCount());
    }

    auto coordinator = std::make_shared<ParallelReplicasReadingCoordinator>(all_replicas_count);
    auto external_tables = new_context->getExternalTables();
    auto read_from_remote = std::make_unique<ReadFromParallelRemoteReplicasStep>(
        query_ast,
        new_cluster,
        std::move(coordinator),
        stream_factory.header,
        stream_factory.processed_stage,
        main_table,
        table_func_ptr,
        new_context,
        getThrottler(new_context),
        std::move(scalars),
        std::move(external_tables),
        &Poco::Logger::get("ReadFromParallelRemoteReplicasStep"),
        query_info.storage_limits);

    query_plan.addStep(std::move(read_from_remote));
}

}

}
