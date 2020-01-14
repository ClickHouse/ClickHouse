#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/queryToString.h>
#include <Interpreters/ProcessList.h>


namespace DB
{

namespace ClusterProxy
{

Context removeUserRestrictionsFromSettings(const Context & context, const Settings & settings)
{
    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// Does not matter on remote servers, because queries are sent under different user.
    new_settings.max_concurrent_queries_for_user = 0;
    new_settings.max_memory_usage_for_user = 0;
    /// This setting is really not for user and should not be sent to remote server.
    new_settings.max_memory_usage_for_all_queries = 0;

    /// Set as unchanged to avoid sending to remote server.
    new_settings.max_concurrent_queries_for_user.changed = false;
    new_settings.max_memory_usage_for_user.changed = false;
    new_settings.max_memory_usage_for_all_queries.changed = false;

    Context new_context(context);
    new_context.setSettings(new_settings);

    return new_context;
}

}

}
