#include <Interpreters/SystemLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/PartLog.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

SystemLogs::SystemLogs(Context & global_context, const Poco::Util::AbstractConfiguration & config)
{
    {
        String database = config.getString("query_log.database", "system");
        String table = config.getString("query_log.table", "query_log");
        String partition_by = config.getString("query_log.partition_by", "toYYYYMM(event_date)");
        size_t flush_interval_milliseconds = config.getUInt64("query_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);

        String engine = "ENGINE = MergeTree PARTITION BY (" + partition_by + ") ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024";

        query_log = std::make_unique<QueryLog>(global_context, database, table, engine, flush_interval_milliseconds);
    }

    if (config.has("part_log"))
    {
        part_log_database = config.getString("part_log.database", "system");

        String table = config.getString("part_log.table", "part_log");
        String partition_by = config.getString("query_log.partition_by", "toYYYYMM(event_date)");
        size_t flush_interval_milliseconds = config.getUInt64("part_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);

        String engine = "ENGINE = MergeTree PARTITION BY (" + partition_by + ") ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024";

        part_log = std::make_unique<PartLog>(global_context, part_log_database, table, engine, flush_interval_milliseconds);
    }
}


SystemLogs::~SystemLogs() = default;

}
