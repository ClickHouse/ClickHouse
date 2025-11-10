#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <Parsers/ASTInsertQuery.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/ParallelReplicasLocalPlan.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/buildQueryTreeForShard.h>
#include <Storages/getStructureOfRemoteTable.h>


namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsUInt64 force_optimize_skip_unused_shards;
    extern const SettingsUInt64 force_optimize_skip_unused_shards_nesting;
    extern const SettingsUInt64 limit;
    extern const SettingsLoadBalancing load_balancing;
    extern const SettingsUInt64 max_concurrent_queries_for_user;
    extern const SettingsUInt64 max_distributed_depth;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsSeconds max_execution_time_leaf;
    extern const SettingsUInt64 max_memory_usage_for_user;
    extern const SettingsUInt64 max_network_bandwidth;
    extern const SettingsUInt64 max_network_bytes;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsUInt64 offset;
    extern const SettingsBool optimize_skip_unused_shards;
    extern const SettingsUInt64 optimize_skip_unused_shards_nesting;
    extern const SettingsBool optimize_skip_unused_shards_rewrite_in;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsUInt64 parallel_replicas_custom_key_range_lower;
    extern const SettingsUInt64 parallel_replicas_custom_key_range_upper;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsMilliseconds queue_max_wait_ms;
    extern const SettingsBool skip_unavailable_shards;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsOverflowMode timeout_overflow_mode_leaf;
    extern const SettingsBool use_hedged_requests;
    extern const SettingsBool serialize_query_plan;
    extern const SettingsBool async_socket_for_remote;
    extern const SettingsBool async_query_sending_for_remote;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool parallel_replicas_support_projection;
}

namespace DistributedSetting
{
    extern const DistributedSettingsBool skip_unavailable_shards;
}

namespace ErrorCodes
{
    extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_CLUSTER;
    extern const int INCONSISTENT_CLUSTER_DEFINITION;
    extern const int NOT_IMPLEMENTED;
}

namespace ClusterProxy
{

ContextMutablePtr updateSettingsAndClientInfoForCluster(const Cluster & cluster,
    bool is_remote_function,
    ContextPtr context,
    const Settings & settings,
    const StorageID & main_table,
    ASTPtr additional_filter_ast,
    LoggerPtr log,
    const DistributedSettings * distributed_settings)
{
    ClientInfo new_client_info = context->getClientInfo();
    Settings new_settings {settings};
    new_settings[Setting::queue_max_wait_ms] = Cluster::saturate(new_settings[Setting::queue_max_wait_ms], settings[Setting::max_execution_time]);

    /// In case of interserver mode we should reset initial_user for remote() function to use passed user from the query.
    if (is_remote_function)
    {
        const auto & address = cluster.getShardsAddresses().front().front();
        new_client_info.initial_user = address.user;
    }

    /// If "secret" (in remote_servers) is not in use,
    /// user on the shard is not the same as the user on the initiator,
    /// hence per-user limits should not be applied.
    const bool interserver_mode = !cluster.getSecret().empty();
    if (!interserver_mode)
    {
        /// Does not matter on remote servers, because queries are sent under different user.
        new_settings[Setting::max_concurrent_queries_for_user] = 0;
        new_settings[Setting::max_memory_usage_for_user] = 0;

        /// Set as unchanged to avoid sending to remote server.
        new_settings[Setting::max_concurrent_queries_for_user].changed = false;
        new_settings[Setting::max_memory_usage_for_user].changed = false;
    }

    if (settings[Setting::force_optimize_skip_unused_shards_nesting] && settings[Setting::force_optimize_skip_unused_shards])
    {
        if (new_settings[Setting::force_optimize_skip_unused_shards_nesting] == 1)
        {
            new_settings[Setting::force_optimize_skip_unused_shards] = false;
            new_settings[Setting::force_optimize_skip_unused_shards].changed = false;

            if (log)
                LOG_TRACE(log, "Disabling force_optimize_skip_unused_shards for nested queries (force_optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings[Setting::force_optimize_skip_unused_shards_nesting].value;
            new_settings[Setting::force_optimize_skip_unused_shards_nesting].changed = true;

            if (log)
                LOG_TRACE(
                    log, "force_optimize_skip_unused_shards_nesting is now {}", new_settings[Setting::force_optimize_skip_unused_shards_nesting].value);
        }
    }

    if (settings[Setting::optimize_skip_unused_shards_nesting] && settings[Setting::optimize_skip_unused_shards])
    {
        if (new_settings[Setting::optimize_skip_unused_shards_nesting] == 1)
        {
            new_settings[Setting::optimize_skip_unused_shards] = false;
            new_settings[Setting::optimize_skip_unused_shards].changed = false;

            if (log)
                LOG_TRACE(log, "Disabling optimize_skip_unused_shards for nested queries (optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings[Setting::optimize_skip_unused_shards_nesting].value;
            new_settings[Setting::optimize_skip_unused_shards_nesting].changed = true;

            if (log)
                LOG_TRACE(log, "optimize_skip_unused_shards_nesting is now {}", new_settings[Setting::optimize_skip_unused_shards_nesting].value);
        }
    }

    if (!settings[Setting::skip_unavailable_shards].changed && distributed_settings)
    {
        new_settings[Setting::skip_unavailable_shards] = (*distributed_settings)[DistributedSetting::skip_unavailable_shards].value;
        new_settings[Setting::skip_unavailable_shards].changed = true;
    }

    if (settings[Setting::offset])
    {
        new_settings[Setting::offset] = 0;
        new_settings[Setting::offset].changed = false;
    }
    if (settings[Setting::limit])
    {
        new_settings[Setting::limit] = 0;
        new_settings[Setting::limit].changed = false;
    }

    /// Setting additional_table_filters may be applied to Distributed table.
    /// In case if query is executed up to WithMergableState on remote shard, it is impossible to filter on initiator.
    /// We need to propagate the setting, but change the table name from distributed to source.
    ///
    /// Here we don't try to analyze setting again. In case if query_info->additional_filter_ast is not empty, some filter was applied.
    /// It's just easier to add this filter for a source table.
    if (additional_filter_ast)
    {
        Tuple tuple;
        tuple.push_back(main_table.getShortName());
        tuple.push_back(additional_filter_ast->formatWithSecretsOneLine());
        new_settings[Setting::additional_table_filters].value.push_back(std::move(tuple));
    }

    /// disable parallel replicas if cluster contains only shards with 1 replica
    if (context->canUseTaskBasedParallelReplicas())
    {
        bool disable_parallel_replicas = false;
        if (is_remote_function)
        {
            if (cluster.getName().empty()) // disable parallel replicas with remote() table functions w/o configured cluster
                disable_parallel_replicas = true;
            else
                new_settings[Setting::cluster_for_parallel_replicas] = cluster.getName();
        }

        if (!disable_parallel_replicas)
        {
            disable_parallel_replicas = true;
            for (const auto & shard : cluster.getShardsInfo())
            {
                if (shard.getAllNodeCount() > 1)
                {
                    disable_parallel_replicas = false;
                    break;
                }
            }
        }

        if (disable_parallel_replicas)
            new_settings[Setting::allow_experimental_parallel_reading_from_replicas] = 0;
    }

    if (settings[Setting::max_execution_time_leaf].value > 0)
    {
        /// Replace 'max_execution_time' of this sub-query with 'max_execution_time_leaf' and 'timeout_overflow_mode'
        /// with 'timeout_overflow_mode_leaf'
        new_settings[Setting::max_execution_time] = settings[Setting::max_execution_time_leaf];
        new_settings[Setting::timeout_overflow_mode] = settings[Setting::timeout_overflow_mode_leaf];
    }

    /// in case of parallel replicas custom key use round robing load balancing
    /// so custom key partitions will be spread over nodes in round-robin fashion
    if (context->canUseParallelReplicasCustomKeyForCluster(cluster) && !settings[Setting::load_balancing].changed)
    {
        new_settings[Setting::load_balancing] = LoadBalancing::ROUND_ROBIN;
    }

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    new_context->setClientInfo(new_client_info);

    if (context->canUseParallelReplicasCustomKeyForCluster(cluster))
        new_context->disableOffsetParallelReplicas();

    return new_context;
}

ContextMutablePtr updateSettingsForCluster(const Cluster & cluster, ContextPtr context, const Settings & settings, const StorageID & main_table)
{
    return updateSettingsAndClientInfoForCluster(cluster,
        /* is_remote_function= */ false,
        context,
        settings,
        main_table,
        /* additional_filter_ast= */ {},
        /* log= */ {},
        /* distributed_settings= */ {});
}


static ThrottlerPtr getThrottler(const ContextPtr & context)
{
    const Settings & settings = context->getSettingsRef();

    ThrottlerPtr user_level_throttler;
    if (auto process_list_element = context->getProcessListElement())
        user_level_throttler = process_list_element->getUserNetworkThrottler();

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings[Setting::max_network_bandwidth] || settings[Setting::max_network_bytes])
    {
        throttler = std::make_shared<Throttler>(
            settings[Setting::max_network_bandwidth],
            settings[Setting::max_network_bytes],
            "Limit for bytes to send or receive over network exceeded.",
            user_level_throttler);
    }
    else
        throttler = user_level_throttler;

    return throttler;
}

AdditionalShardFilterGenerator
getShardFilterGeneratorForCustomKey(const Cluster & cluster, ContextPtr context, const ColumnsDescription & columns)
{
    if (!context->canUseParallelReplicasCustomKeyForCluster(cluster))
        return {};

    const auto & settings = context->getSettingsRef();
    auto custom_key_ast = parseCustomKeyForTable(settings[Setting::parallel_replicas_custom_key], *context);
    if (custom_key_ast == nullptr)
        return {};

    return [my_custom_key_ast = std::move(custom_key_ast),
            column_description = columns,
            custom_key_type = settings[Setting::parallel_replicas_mode].value,
            custom_key_range_lower = settings[Setting::parallel_replicas_custom_key_range_lower].value,
            custom_key_range_upper = settings[Setting::parallel_replicas_custom_key_range_upper].value,
            query_context = context,
            replica_count = cluster.getShardsInfo().front().per_replica_pools.size()](uint64_t replica_num) -> ASTPtr
    {
        return getCustomKeyFilterForParallelReplica(
            replica_count,
            replica_num - 1,
            my_custom_key_ast,
            {custom_key_type, custom_key_range_lower, custom_key_range_upper},
            column_description,
            query_context);
    };
}


void executeQuery(
    QueryPlan & query_plan,
    SharedHeader header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    LoggerPtr log,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const DistributedSettings & distributed_settings,
    AdditionalShardFilterGenerator shard_filter_generator,
    bool is_remote_function)
{
    const Settings & settings = context->getSettingsRef();

    if (settings[Setting::max_distributed_depth] && context->getClientInfo().distributed_depth >= settings[Setting::max_distributed_depth])
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    const ClusterPtr & not_optimized_cluster = query_info.cluster;

    std::vector<QueryPlanPtr> plans;
    SelectStreamFactory::Shards remote_shards;

    auto cluster = query_info.getCluster();
    auto new_context = updateSettingsAndClientInfoForCluster(*cluster, is_remote_function, context,
        settings, main_table, query_info.additional_filter_ast, log, &distributed_settings);
    if (context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas].value
        && context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas].value
           != new_context->getSettingsRef()[Setting::allow_experimental_parallel_reading_from_replicas].value)
    {
        LOG_TRACE(
            log,
            "Parallel reading from replicas is disabled for cluster. There are no shards with more than 1 replica: cluster={}",
            cluster->getName());
    }

    new_context->increaseDistributedDepth();

    const size_t shards = cluster->getShardCount();

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        for (size_t i = 0, s = cluster->getShardsInfo().size(); i < s; ++i)
        {
            const auto & shard_info = cluster->getShardsInfo()[i];

            auto query_for_shard = query_info.query_tree->clone();
            if (sharding_key_expr && query_info.optimized_cluster && settings[Setting::optimize_skip_unused_shards_rewrite_in] && shards > 1 &&
                /// TODO: support composite sharding key
                sharding_key_expr->getRequiredColumns().size() == 1)
            {
                OptimizeShardingKeyRewriteInVisitor::Data visitor_data{
                    sharding_key_expr,
                    sharding_key_column_name,
                    shard_info,
                    not_optimized_cluster->getSlotToShard(),
                };
                optimizeShardingKeyRewriteIn(query_for_shard, std::move(visitor_data), new_context);
            }

            // decide for each shard if parallel reading from replicas should be enabled
            // according to settings and number of replicas declared per shard
            const auto & addresses = cluster->getShardsAddresses().at(i);
            const bool parallel_replicas_enabled = addresses.size() > 1 && new_context->canUseTaskBasedParallelReplicas();

            stream_factory.createForShard(
                shard_info,
                query_for_shard,
                main_table,
                table_func_ptr,
                new_context,
                plans,
                remote_shards,
                static_cast<UInt32>(shards),
                parallel_replicas_enabled,
                shard_filter_generator);
        }
    }
    else
    {
        for (size_t i = 0, s = cluster->getShardsInfo().size(); i < s; ++i)
        {
            const auto & shard_info = cluster->getShardsInfo()[i];

            ASTPtr query_ast_for_shard = query_info.query->clone();
            if (sharding_key_expr && query_info.optimized_cluster && settings[Setting::optimize_skip_unused_shards_rewrite_in] && shards > 1 &&
                /// TODO: support composite sharding key
                sharding_key_expr->getRequiredColumns().size() == 1)
            {
                OptimizeShardingKeyRewriteInVisitor::Data visitor_data{
                    sharding_key_expr,
                    sharding_key_column_name,
                    shard_info,
                    not_optimized_cluster->getSlotToShard(),
                };
                OptimizeShardingKeyRewriteInVisitor visitor(visitor_data);
                visitor.visit(query_ast_for_shard);
            }

            // decide for each shard if parallel reading from replicas should be enabled
            // according to settings and number of replicas declared per shard
            const auto & addresses = cluster->getShardsAddresses().at(i);
            bool parallel_replicas_enabled = addresses.size() > 1 && context->canUseTaskBasedParallelReplicas();

            stream_factory.createForShard(
                shard_info,
                query_ast_for_shard,
                main_table,
                table_func_ptr,
                new_context,
                plans,
                remote_shards,
                static_cast<UInt32>(shards),
                parallel_replicas_enabled,
                shard_filter_generator);
        }
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
            not_optimized_cluster->getName());

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

    SharedHeaders input_headers;
    input_headers.reserve(plans.size());
    for (auto & plan : plans)
        input_headers.emplace_back(plan->getCurrentHeader());

    auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

static ContextMutablePtr updateContextForParallelReplicas(const LoggerPtr & logger, const ContextPtr & context, const UInt64 & shard_num)
{
    const auto & settings = context->getSettingsRef();
    auto context_mutable = Context::createCopy(context);

    /// check hedged connections setting
    if (settings[Setting::use_hedged_requests].value)
    {
        if (settings[Setting::use_hedged_requests].changed)
        {
            LOG_WARNING(
                logger,
                "Setting 'use_hedged_requests' explicitly with enabled 'enable_parallel_replicas' has no effect. "
                "Hedged connections are not used for parallel reading from replicas");
        }
        else
        {
            LOG_INFO(
                logger,
                "Disabling 'use_hedged_requests' in favor of 'enable_parallel_replicas'. Hedged connections are "
                "not used for parallel reading from replicas");
        }

        /// disable hedged connections -> parallel replicas uses own logic to choose replicas
        context_mutable->setSetting("use_hedged_requests", Field{false});
    }

    /// If parallel replicas executed over distributed table i.e. in scope of a shard,
    /// currently, local plan for parallel replicas is not created.
    /// Having local plan is prerequisite to use projection with parallel replicas.
    /// So, currently, we disable projection support with parallel replicas when reading over distributed table with parallel replicas
    /// Otherwise, it can lead to incorrect results, in particular with use of implicit projection min_max_count
    if (shard_num > 0 && settings[Setting::parallel_replicas_support_projection].value)
    {
        LOG_TRACE(
            logger,
            "Disabling 'parallel_replicas_support_projection'. Currently, it's not supported for queries with parallel replicas over distributed tables");
        context_mutable->setSetting("parallel_replicas_support_projection", Field{false});
    }

    return context_mutable;
}

static std::pair<ClusterPtr, size_t> prepareClusterForParallelReplicas(const LoggerPtr & logger, const ContextPtr & context)
{
    /// check cluster for parallel replicas
    auto not_optimized_cluster = context->getClusterForParallelReplicas();

    auto scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    UInt64 shard_num = 0; /// shard_num is 1-based, so 0 - no shard specified
    const auto it = scalars.find("_shard_num");
    if (it != scalars.end())
    {
        const Block & block = it->second;
        const auto & column = block.safeGetByPosition(0).column;
        shard_num = column->getUInt(0);
    }

    ClusterPtr new_cluster = not_optimized_cluster;
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

        LOG_DEBUG(logger, "Parallel replicas query in shard scope: shard_num={} cluster={}", shard_num, not_optimized_cluster->getName());

        // get cluster for shard specified by shard_num
        // shard_num is 1-based, but getClusterWithSingleShard expects 0-based index
        new_cluster = not_optimized_cluster->getClusterWithSingleShard(shard_num - 1);
    }
    else
    {
        if (not_optimized_cluster->getShardCount() > 1)
            throw DB::Exception(
                ErrorCodes::UNEXPECTED_CLUSTER,
                "`cluster_for_parallel_replicas` setting refers to cluster with several shards. Expected a cluster with one shard");
    }

    return {new_cluster, shard_num};
}

static std::pair<std::vector<ConnectionPoolPtr>, size_t> prepareConnectionPoolsForParallelReplicas(const LoggerPtr & logger, const ContextPtr & context, const ClusterPtr & cluster)
{
    const auto & settings = context->getSettingsRef();

    const auto & shard = cluster->getShardsInfo().at(0);
    size_t max_replicas_to_use = settings[Setting::max_parallel_replicas];
    if (max_replicas_to_use > shard.getAllNodeCount())
    {
        LOG_TRACE(
            logger,
            "The number of replicas requested ({}) is bigger than the real number available in the cluster ({}). "
            "Will use the latter number to execute the query.",
            settings[Setting::max_parallel_replicas].value,
            shard.getAllNodeCount());
        max_replicas_to_use = shard.getAllNodeCount();
    }

    std::vector<ConnectionPoolWithFailover::Base::ShuffledPool> shuffled_pool;
    if (max_replicas_to_use < shard.getAllNodeCount())
    {
        // will be shuffled according to `load_balancing` setting
        shuffled_pool = shard.pool->getShuffledPools(settings);
    }
    else
    {
        /// If all replicas in cluster are used for query execution,
        /// try to preserve replicas order as in cluster definition.
        /// It's important for data locality during query execution
        /// independently of the query initiator
        auto priority_func = [](size_t i) { return Priority{static_cast<Int64>(i)}; };
        shuffled_pool = shard.pool->getShuffledPools(settings, priority_func);
    }

    std::vector<ConnectionPoolPtr> pools_to_use;
    pools_to_use.reserve(shuffled_pool.size());
    for (auto & pool : shuffled_pool)
        pools_to_use.emplace_back(std::move(pool.pool));

    return {pools_to_use, max_replicas_to_use};
}

static std::optional<size_t> findLocalReplicaIndexAndUpdatePools(std::vector<ConnectionPoolPtr> & pools, size_t max_replicas_to_use, const ClusterPtr & cluster)
{
    const auto & shard = cluster->getShardsInfo().at(0);

    /// find local replica index in pool
    std::optional<size_t> local_replica_index;
    for (size_t i = 0, s = pools.size(); i < s; ++i)
    {
        const auto & hostname = pools[i]->getHost();
        const auto & port = pools[i]->getPort();
        const auto found = std::find_if(
            begin(shard.local_addresses),
            end(shard.local_addresses),
            [&hostname, &port](const Cluster::Address & local_addr)
            {
                return hostname == local_addr.host_name && port == local_addr.port;
            });
        if (found != shard.local_addresses.end())
        {
            local_replica_index = i;
            break;
        }
    }
    if (!local_replica_index)
        throw Exception(
            ErrorCodes::INCONSISTENT_CLUSTER_DEFINITION,
            "Local replica is not found in '{}' cluster definition, see 'cluster_for_parallel_replicas' setting",
            cluster->getName());

    // resize the pool but keep local replicas in it (and update its index)
    chassert(max_replicas_to_use <= pools.size());
    if (local_replica_index >= max_replicas_to_use)
    {
        std::swap(pools[max_replicas_to_use - 1], pools[local_replica_index.value()]);
        local_replica_index = max_replicas_to_use - 1;
    }
    pools.resize(max_replicas_to_use);
    return local_replica_index;
}

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    SharedHeader header,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    QueryPlanStepPtr analyzed_read_from_merge_tree)
{
    auto logger = getLogger("executeQueryWithParallelReplicas");
    LOG_DEBUG(logger, "Executing read from {}, header {}, query ({}), stage {} with parallel replicas",
        storage_id.getNameForLogs(), header->dumpStructure(), query_ast->formatForLogging(), processed_stage);

    auto [cluster, shard_num] = prepareClusterForParallelReplicas(logger, context);
    auto new_context = updateContextForParallelReplicas(logger, context, shard_num);
    auto [connection_pools, max_replicas_to_use] = prepareConnectionPoolsForParallelReplicas(logger, new_context, cluster);

    auto external_tables = new_context->getExternalTables();
    auto coordinator = std::make_shared<ParallelReplicasReadingCoordinator>(max_replicas_to_use);
    auto scalars = new_context->hasQueryContext() ? new_context->getQueryContext()->getScalars() : Scalars{};
    const auto & shard = cluster->getShardsInfo().at(0);

    const auto & settings = new_context->getSettingsRef();
    /// do not build local plan for distributed queries for now (address it later)
    if (settings[Setting::allow_experimental_analyzer] && settings[Setting::parallel_replicas_local_plan] && !shard_num)
    {
        auto local_replica_index = findLocalReplicaIndexAndUpdatePools(connection_pools, max_replicas_to_use, cluster);

        auto [local_plan, with_parallel_replicas] = createLocalPlanForParallelReplicas(
            query_ast,
            *header,
            new_context,
            processed_stage,
            coordinator,
            std::move(analyzed_read_from_merge_tree),
            local_replica_index.value());

        if (!with_parallel_replicas || connection_pools.size() == 1)
        {
            query_plan = std::move(*local_plan);
            return;
        }

        auto read_from_local = std::make_unique<ReadFromLocalParallelReplicaStep>(std::move(local_plan));
        auto stub_local_plan = std::make_unique<QueryPlan>();
        stub_local_plan->addStep(std::move(read_from_local));

        LOG_DEBUG(logger, "Local replica got replica number {}", local_replica_index.value());

        auto read_from_remote = std::make_unique<ReadFromParallelRemoteReplicasStep>(
            query_ast,
            cluster,
            storage_id,
            coordinator,
            header,
            processed_stage,
            new_context,
            getThrottler(new_context),
            std::move(scalars),
            std::move(external_tables),
            getLogger("ReadFromParallelRemoteReplicasStep"),
            std::move(storage_limits),
            std::move(connection_pools),
            local_replica_index,
            shard.pool);

        auto remote_plan = std::make_unique<QueryPlan>();
        remote_plan->addStep(std::move(read_from_remote));

        SharedHeaders input_headers;
        input_headers.reserve(2);
        input_headers.emplace_back(stub_local_plan->getCurrentHeader());
        input_headers.emplace_back(remote_plan->getCurrentHeader());

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(stub_local_plan));
        plans.emplace_back(std::move(remote_plan));

        auto union_step = std::make_unique<UnionStep>(std::move(input_headers));
        query_plan.unitePlans(std::move(union_step), std::move(plans));
    }
    else
    {
        chassert(max_replicas_to_use <= connection_pools.size());
        connection_pools.resize(max_replicas_to_use);

        auto read_from_remote = std::make_unique<ReadFromParallelRemoteReplicasStep>(
            query_ast,
            cluster,
            storage_id,
            std::move(coordinator),
            header,
            processed_stage,
            new_context,
            getThrottler(new_context),
            std::move(scalars),
            std::move(external_tables),
            getLogger("ReadFromParallelRemoteReplicasStep"),
            std::move(storage_limits),
            std::move(connection_pools),
            std::nullopt,
            shard.pool);

        query_plan.addStep(std::move(read_from_remote));
    }
}

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    QueryProcessingStage::Enum processed_stage,
    const QueryTreeNodePtr & query_tree,
    const PlannerContextPtr & planner_context,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits,
    QueryPlanStepPtr analyzed_read_from_merge_tree)
{
    QueryTreeNodePtr modified_query_tree = query_tree->clone();
    rewriteJoinToGlobalJoin(modified_query_tree, context);
    modified_query_tree = buildQueryTreeForShard(planner_context, modified_query_tree, /*allow_global_join_for_right_table*/ true);

    auto header
        = InterpreterSelectQueryAnalyzer::getSampleBlock(modified_query_tree, context, SelectQueryOptions(processed_stage).analyze());
    auto modified_query_ast = queryNodeToDistributedSelectQuery(modified_query_tree);

    executeQueryWithParallelReplicas(
        query_plan, storage_id, header, processed_stage, modified_query_ast, context, storage_limits, std::move(analyzed_read_from_merge_tree));
}

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context,
    std::shared_ptr<const StorageLimitsList> storage_limits)
{
    auto modified_query_ast = ClusterProxy::rewriteSelectQuery(
        context, query_ast, storage_id.database_name, storage_id.table_name, /*remote_table_function_ptr*/ nullptr);
    auto header = InterpreterSelectQuery(modified_query_ast, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    executeQueryWithParallelReplicas(query_plan, storage_id, header, processed_stage, modified_query_ast, context, storage_limits, nullptr);
}

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    SharedHeader header,
    ContextPtr context)
{
    /// Return directly (with correct header) if no shard to query.
    if (query_info.getCluster()->getShardsInfo().empty())
    {
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            return;

        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Distributed)");
        query_plan.addStep(std::move(read_from_pipe));
        return;
    }

    ColumnsDescriptionByShardNum columns_object;
    if (hasDynamicSubcolumnsDeprecated(columns))
        columns_object = getExtendedObjectsOfRemoteTables(*query_info.cluster, storage_id, columns, context);

    ClusterProxy::SelectStreamFactory select_stream_factory
        = ClusterProxy::SelectStreamFactory(header, columns_object, snapshot, processed_stage);

    auto shard_filter_generator = getShardFilterGeneratorForCustomKey(*query_info.getCluster(), context, columns);
    if (shard_filter_generator && context->getSettingsRef()[Setting::serialize_query_plan])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel replicas with custom key are not supported with serialize_query_plan enabled");

    ClusterProxy::executeQuery(
        query_plan,
        header,
        processed_stage,
        storage_id,
        /*table_func_ptr=*/nullptr,
        select_stream_factory,
        getLogger("executeQueryWithParallelReplicasCustomKey"),
        context,
        query_info,
        /*sharding_key_expr=*/nullptr,
        /*sharding_key_column_name=*/{},
        /*distributed_settings=*/{},
        shard_filter_generator,
        /*is_remote_function=*/false);
}

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    const SelectQueryInfo & query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    const QueryTreeNodePtr & query_tree,
    ContextPtr context)
{
    auto header = InterpreterSelectQueryAnalyzer::getSampleBlock(query_tree, context, SelectQueryOptions(processed_stage).analyze());
    executeQueryWithParallelReplicasCustomKey(query_plan, storage_id, query_info, columns, snapshot, processed_stage, header, context);
}

void executeQueryWithParallelReplicasCustomKey(
    QueryPlan & query_plan,
    const StorageID & storage_id,
    SelectQueryInfo query_info,
    const ColumnsDescription & columns,
    const StorageSnapshotPtr & snapshot,
    QueryProcessingStage::Enum processed_stage,
    const ASTPtr & query_ast,
    ContextPtr context)
{
    auto header = InterpreterSelectQuery(query_ast, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();
    query_info.query = ClusterProxy::rewriteSelectQuery(
        context, query_info.query, storage_id.getDatabaseName(), storage_id.getTableName(), /*table_function_ptr=*/nullptr);
    executeQueryWithParallelReplicasCustomKey(query_plan, storage_id, query_info, columns, snapshot, processed_stage, header, context);
}

bool canUseParallelReplicasOnInitiator(const ContextPtr & context)
{
    if (!context->canUseParallelReplicasOnInitiator())
        return false;

    auto cluster = context->getClusterForParallelReplicas();
    if (cluster->getShardCount() == 1)
        return cluster->getShardsInfo()[0].getAllNodeCount() > 1;

    /// parallel replicas with distributed table
    auto scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
    UInt64 shard_num = 0; /// shard_num is 1-based, so 0 - no shard specified
    const auto it = scalars.find("_shard_num");
    if (it != scalars.end())
    {
        const Block & block = it->second;
        const auto & column = block.safeGetByPosition(0).column;
        shard_num = column->getUInt(0);
    }
    if (shard_num > 0)
    {
        const auto shard_count = cluster->getShardCount();
        if (shard_num > shard_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Shard number is greater than shard count: shard_num={} shard_count={} cluster={}",
                shard_num,
                shard_count,
                cluster->getName());

        return cluster->getShardsInfo().at(shard_num - 1).getAllNodeCount() > 1;
    }

    if (cluster->getShardCount() > 1)
        throw DB::Exception(
            ErrorCodes::UNEXPECTED_CLUSTER,
            "`cluster_for_parallel_replicas` setting refers to cluster with {} shards. Expected a cluster with one shard",
            cluster->getShardCount());

    return false;
}

bool isSuitableForParallelReplicas(const ASTPtr & select, const ContextPtr & context)
{
    auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

    InterpreterSelectQueryAnalyzer interpreter(select, context, select_query_options);
    auto & plan = interpreter.getQueryPlan();

    auto is_reading_with_parallel_replicas = [](const QueryPlan::Node * node) -> bool
    {
        struct Frame
        {
            const QueryPlan::Node * node = nullptr;
            size_t next_child = 0;
        };

        using Stack = std::vector<Frame>;

        Stack stack;
        stack.push_back(Frame{.node = node});

        while (!stack.empty())
        {
            auto & frame = stack.back();

            if (frame.node->children.empty())
            {
                const auto * step = frame.node->step.get();
                if (typeid_cast<const ReadFromParallelRemoteReplicasStep *>(step))
                    return true;
            }

            /// Traverse all children first.
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(next_frame);
                continue;
            }

            stack.pop_back();
        }

        return false;
    };

    return is_reading_with_parallel_replicas(plan.getRootNode());
}

/// find and remove ReadFromParallelRemoteReplicasStep in query plan,
/// also returns parallel replicas coordinator stored in ReadFromParallelRemoteReplicasStep
ParallelReplicasReadingCoordinatorPtr dropReadFromRemoteInPlan(QueryPlan & query_plan)
{
    struct Frame
    {
        QueryPlan::Node * node = nullptr;
        QueryPlan::Node * parent = nullptr;
        size_t next_child = 0;
    };

    using Stack = std::vector<Frame>;

    Stack stack;
    stack.push_back(Frame{.node = query_plan.getRootNode()});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.node->children.empty())
        {
            const auto * step = frame.node->step.get();
            if (const auto * read_from_remote = typeid_cast<const ReadFromParallelRemoteReplicasStep *>(step))
            {
                auto & children = frame.parent->children;
                for (auto it = begin(children); it != end(children); ++it)
                {
                    if ((*it)->step.get() == step)
                    {
                        children.erase(it);
                        return read_from_remote->getCoordinator();
                    }
                }
            }
        }

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child], .parent = frame.node};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        stack.pop_back();
    }

    return nullptr;
}

std::optional<QueryPipeline> executeInsertSelectWithParallelReplicas(
    const ASTInsertQuery & query_ast,
    const ContextPtr & context,
    std::optional<QueryPipeline> local_pipeline,
    std::optional<ParallelReplicasReadingCoordinatorPtr> coordinator)
{
    auto logger = getLogger("executeInsertSelectWithParallelReplicas");
    LOG_DEBUG(logger, "Executing query with parallel replicas: {}", query_ast.formatForLogging());

    const auto & settings = context->getSettingsRef();

    auto [cluster, shard_num] = prepareClusterForParallelReplicas(logger, context);
    auto new_context = updateContextForParallelReplicas(logger, context, shard_num);
    auto [connection_pools, max_replicas_to_use] = prepareConnectionPoolsForParallelReplicas(logger, new_context, cluster);
    std::optional<size_t> local_replica_index;

    if (coordinator)
    {
        chassert(local_pipeline);

        local_replica_index = findLocalReplicaIndexAndUpdatePools(connection_pools, max_replicas_to_use, cluster);
        chassert(local_replica_index.has_value());

        /// while building local pipeline
        /// - the coordinator is created
        /// - the coordinator got announcement from local replica and replica number is assigned to it
        /// (since the coordinator got first announcement from local replica, - its snapshot will be used for query execution)
        /// so, here, we need to reuse already assigned number to local replica
        auto snapshot_replica_num = (*coordinator)->getSnapshotReplicaNum();
        chassert(snapshot_replica_num.has_value());

        if (local_replica_index.value() != snapshot_replica_num.value())
        {
            std::swap(connection_pools[local_replica_index.value()], connection_pools[snapshot_replica_num.value()]);
            local_replica_index = snapshot_replica_num;
        }

        LOG_DEBUG(logger, "Local replica got replica number {}", local_replica_index.value());
    }
    connection_pools.resize(max_replicas_to_use);

    String formatted_query;
    {
        InterpreterSelectQueryAnalyzer analyzer(query_ast.select, new_context, {});
        const auto & query_tree = analyzer.getQueryTree();
        auto select_ast = query_tree->toAST();

        auto new_query_ast = query_ast.clone();
        auto * insert_ast = new_query_ast->as<ASTInsertQuery>();
        insert_ast->select = std::move(select_ast);

        WriteBufferFromOwnString buf;
        IAST::FormatSettings ast_format_settings(
            /*one_line=*/true, /*identifier_quoting_rule=*/IdentifierQuotingRule::Always);
        insert_ast->IAST::format(buf, ast_format_settings);
        formatted_query = buf.str();
    }

    QueryPipeline pipeline;
    if (local_pipeline)
    {
        chassert(coordinator.has_value());
        pipeline.addCompletedPipeline(std::move(*local_pipeline));
    }

    if (!coordinator)
        coordinator = std::make_shared<ParallelReplicasReadingCoordinator>(max_replicas_to_use);

    for (size_t i = 0; i < connection_pools.size(); ++i)
    {
        if (local_replica_index && i == *local_replica_index)
            continue;

        IConnections::ReplicaInfo replica_info{
            /// we should use this number specifically because efficiency of data distribution by consistent hash depends on it.
            .number_of_current_replica = i,
        };

        const ThrottlerPtr null_throttler; /// so no need for throttler since no table data will transferred between replicas
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            connection_pools[i],
            formatted_query,
            std::make_shared<const Block>(Block{}),
            new_context,
            null_throttler,
            Scalars{},
            Tables{},
            QueryProcessingStage::Complete,
            RemoteQueryExecutor::Extension{.parallel_reading_coordinator = *coordinator, .replica_info = replica_info});
        remote_query_executor->setLogger(logger);
        // TODO: check if source table is present on remote, similar to reading with PR

        QueryPipeline remote_pipeline(std::make_shared<RemoteSource>(
            remote_query_executor, false, settings[Setting::async_socket_for_remote], settings[Setting::async_query_sending_for_remote]));
        remote_pipeline.complete(std::make_shared<EmptySink>(remote_query_executor->getSharedHeader()));

        pipeline.addCompletedPipeline(std::move(remote_pipeline));
    }

    /// Otherwise CompletedPipelineExecutor uses 1 thread by default
    pipeline.setNumThreads(settings[Setting::max_threads]);

    return pipeline;
}

}

}
