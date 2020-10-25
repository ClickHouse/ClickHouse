#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/queryToString.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Pipe.h>


namespace DB
{

namespace ClusterProxy
{

Context removeUserRestrictionsFromSettings(const Context & context, const Settings & settings, Poco::Logger * log)
{
    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// Does not matter on remote servers, because queries are sent under different user.
    new_settings.max_concurrent_queries_for_user = 0;
    new_settings.max_memory_usage_for_user = 0;

    /// Set as unchanged to avoid sending to remote server.
    new_settings.max_concurrent_queries_for_user.changed = false;
    new_settings.max_memory_usage_for_user.changed = false;

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

    Context new_context(context);
    new_context.setSettings(new_settings);

    return new_context;
}

Pipes executeQuery(
    IStreamFactory & stream_factory, const ClusterPtr & cluster, Poco::Logger * log,
    const ASTPtr & query_ast, const Context & context, const Settings & settings, const SelectQueryInfo & query_info)
{
    assert(log);

    Pipes res;

    const std::string query = queryToString(query_ast);

    Context new_context = removeUserRestrictionsFromSettings(context, settings, log);

    ThrottlerPtr user_level_throttler;
    if (auto * process_list_element = context.getProcessListElement())
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

    for (const auto & shard_info : cluster->getShardsInfo())
        stream_factory.createForShard(shard_info, query, query_ast, new_context, throttler, query_info, res);

    return res;
}

}

}
