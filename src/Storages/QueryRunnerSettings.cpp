#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/QueryRunnerSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define QUERY_RUNNER_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, cluster, "", "Name of the cluster to send the queries to. If empty, the queries are executed locally.", 0) \
    DECLARE(String, shard, "1", "1-based index of the cluster's shard to send the queries to, or 'random' to pick a random shard per query, or 'all' to run each query on every shard. Requires the 'cluster' setting.", 0) \
    DECLARE(QueryRunnerMode, mode, QueryRunnerMode::ASYNCHRONOUS, "If 'synchronous', INSERT returns after all queries of the inserted batch have finished. If 'asynchronous', INSERT returns as soon as the queries are queued.", 0) \
    DECLARE(UInt64, threads, 4, "Number of background threads executing the queries.", 0) \
    DECLARE(UInt64, max_queue_size, 1000, "Maximum number of queued queries. When the queue is full, newly inserted queries are discarded, and an error is logged.", 0) \

DECLARE_SETTINGS_TRAITS(QueryRunnerSettingsTraits, QUERY_RUNNER_SETTINGS, QUERY_RUNNER_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(QueryRunnerSettingsTraits, QUERY_RUNNER_SETTINGS, QueryRunnerSettings, QueryRunnerSetting)

QueryRunnerSettings::QueryRunnerSettings() : impl(std::make_unique<QueryRunnerSettingsImpl>())
{
}

QueryRunnerSettings::QueryRunnerSettings(QueryRunnerSettings && settings) noexcept = default;

QueryRunnerSettings::~QueryRunnerSettings() = default;

QUERY_RUNNER_SETTINGS_SUPPORTED_TYPES(QueryRunnerSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void QueryRunnerSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

bool QueryRunnerSettings::hasBuiltin(std::string_view name)
{
    return QueryRunnerSettingsImpl::hasBuiltin(name);
}

}
