#include <Interpreters/SystemLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/PartLog.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace
{

constexpr size_t DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS = 7500;

/// Creates a system log with MergeTree engine using parameters from config
template <typename TSystemLog>
std::shared_ptr<TSystemLog> createSystemLog(
    Context & context,
    const String & default_database_name,
    const String & default_table_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    if (!config.has(config_prefix))
        return {};

    String database = config.getString(config_prefix + ".database", default_database_name);
    String table = config.getString(config_prefix + ".table", default_table_name);
    String partition_by = config.getString(config_prefix + ".partition_by", "toYYYYMM(event_date)");
    String engine = "ENGINE = MergeTree PARTITION BY (" + partition_by + ") ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024";

    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds", DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    return std::make_shared<TSystemLog>(context, database, table, engine, flush_interval_milliseconds);
}

}


SystemLogs::SystemLogs(Context & global_context, const Poco::Util::AbstractConfiguration & config)
{
    query_log = createSystemLog<QueryLog>(global_context, "system", "query_log", config, "query_log");
    query_thread_log = createSystemLog<QueryThreadLog>(global_context, "system", "query_thread_log", config, "query_thread_log");
    part_log = createSystemLog<PartLog>(global_context, "system", "part_log", config, "part_log");

    part_log_database = config.getString("part_log.database", "system");
}


SystemLogs::~SystemLogs()
{
    if (query_log)
        query_log->shutdown();
    if (query_thread_log)
        query_thread_log->shutdown();
    if (part_log)
        part_log->shutdown();
}

}
