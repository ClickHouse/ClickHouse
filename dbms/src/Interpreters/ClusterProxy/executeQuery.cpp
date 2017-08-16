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
        const ASTPtr & query_ast, const Context & context, const Settings & settings)
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

    Context new_context(context);
    new_context.setSettings(new_settings);

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings.limits.max_network_bandwidth || settings.limits.max_network_bytes)
        throttler = std::make_shared<Throttler>(
            settings.limits.max_network_bandwidth,
            settings.limits.max_network_bytes,
            "Limit for bytes to send or receive over network exceeded.");

    for (const auto & shard_info : cluster->getShardsInfo())
        stream_factory.createForShard(shard_info, query, query_ast, new_context, throttler, res);

    return res;
}

}

}
