#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/queryToString.h>
#include <DataStreams/RemoteBlockInputStream.h>


namespace DB
{

namespace ClusterProxy
{

BlockInputStreams executeQuery(
        IStreamFactory & stream_factory, const ClusterPtr & cluster,
        const ASTPtr & query_ast, const Context & context, const Settings & settings, bool enable_shard_multiplexing)
{
    BlockInputStreams res;

    const std::string query = queryToString(query_ast);

    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

    /// Does not matter on remote servers, because queries are sent under different user.
    new_settings.max_concurrent_queries_for_user = 0;
    new_settings.limits.max_memory_usage_for_user = 0;
    /// This setting is really not for user and should not be sent to remote server.
    new_settings.limits.max_memory_usage_for_all_queries = 0;

    /// Set as unchanged to avoid sending to remote server.
    new_settings.max_concurrent_queries_for_user.changed = false;
    new_settings.limits.max_memory_usage_for_user.changed = false;
    new_settings.limits.max_memory_usage_for_all_queries.changed = false;

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings.limits.max_network_bandwidth || settings.limits.max_network_bytes)
        throttler = std::make_shared<Throttler>(
            settings.limits.max_network_bandwidth,
            settings.limits.max_network_bytes,
            "Limit for bytes to send or receive over network exceeded.");

    /// Spread shards by threads uniformly.

    size_t remote_count = 0;

    if (stream_factory.getPoolMode() == PoolMode::GET_ALL)
    {
        for (const auto & shard_info : cluster->getShardsInfo())
        {
            if (shard_info.hasRemoteConnections())
                ++remote_count;
        }
    }
    else
        remote_count = cluster->getRemoteShardCount();

    size_t thread_count;

    if (!enable_shard_multiplexing)
        thread_count = remote_count;
    else if (remote_count == 0)
        thread_count = 0;
    else if (settings.max_distributed_processing_threads == 0)
        thread_count = 1;
    else
        thread_count = std::min(remote_count, static_cast<size_t>(settings.max_distributed_processing_threads));

    size_t pools_per_thread = (thread_count > 0) ? (remote_count / thread_count) : 0;
    size_t remainder = (thread_count > 0) ? (remote_count % thread_count) : 0;

    ConnectionPoolWithFailoverPtrs pools;

    /// Loop over shards.
    size_t current_thread = 0;
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        bool create_local_queries = shard_info.isLocal();

        bool create_remote_queries;
        if (stream_factory.getPoolMode() == PoolMode::GET_ALL)
            create_remote_queries = shard_info.hasRemoteConnections();
        else
            create_remote_queries = !create_local_queries;

        if (create_local_queries)
        {
            /// Add queries to localhost (they are processed in-process, without network communication).

            Context new_context = context;
            new_context.setSettings(new_settings);

            for (const auto & address : shard_info.local_addresses)
            {
                BlockInputStreamPtr stream = stream_factory.createLocal(query_ast, new_context, address);
                if (stream)
                    res.emplace_back(stream);
            }
        }

        if (create_remote_queries)
        {
            size_t excess = (current_thread < remainder) ? 1 : 0;
            size_t actual_pools_per_thread = pools_per_thread + excess;

            if (actual_pools_per_thread == 1)
            {
                res.emplace_back(stream_factory.createRemote(shard_info.pool, query, new_settings, throttler, context));
                ++current_thread;
            }
            else
            {
                pools.push_back(shard_info.pool);
                if (pools.size() == actual_pools_per_thread)
                {
                    res.emplace_back(stream_factory.createRemote(std::move(pools), query, new_settings, throttler, context));
                    pools = ConnectionPoolWithFailoverPtrs();
                    ++current_thread;
                }
            }
        }
    }

    return res;
}

}

}
