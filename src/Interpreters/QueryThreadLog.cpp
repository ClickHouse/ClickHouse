#include <Interpreters/QueryThreadLog.h>
#include <Common/SystemTableDocumentation.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/QueryLog.h>
#include <Common/ClickHouseRevision.h>


namespace DB
{

ColumnsDescription QueryThreadLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", low_cardinality_string, "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "The date when the thread has finished execution of the query."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "The date and time when the thread has finished execution of the query."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "The date and time when the thread has finished execution of the query with microseconds precision."},
        {"query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of query execution."},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of query execution with microsecond precision."},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of query execution."},

        {"read_rows", std::make_shared<DataTypeUInt64>(), "Number of read rows."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "Number of read bytes."},
        {"written_rows", std::make_shared<DataTypeUInt64>(), "For INSERT queries, the number of written rows. For other queries, the column value is 0."},
        {"written_bytes", std::make_shared<DataTypeUInt64>(), "For INSERT queries, the number of written bytes. For other queries, the column value is 0."},
        {"memory_usage", std::make_shared<DataTypeInt64>(), "The difference between the amount of allocated and freed memory in context of this thread."},
        {"peak_memory_usage", std::make_shared<DataTypeInt64>(), "The maximum difference between the amount of allocated and freed memory in context of this thread."},

        {"thread_name", low_cardinality_string, "Name of the thread."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Internal thread ID."},
        {"master_thread_id", std::make_shared<DataTypeUInt64>(), "OS initial ID of initial thread."},
        {"current_database", low_cardinality_string, "Name of the current database."},
        {"query", std::make_shared<DataTypeString>(), "Query string."},
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>(), "The hash of normalized query - with wiped constants, etc."},

        {"is_initial_query", std::make_shared<DataTypeUInt8>(), "Whether the query is initial. Possible values: 1 — an initial (top-level) query, 0 — a child query initiated by another query, including queries for distributed execution and internal subqueries."},
        {"connection_address", DataTypeFactory::instance().get("IPv6"), "The client IP address from which the connection was made. When connected through a proxy, this will be the address of the proxy."},
        {"connection_port", std::make_shared<DataTypeUInt16>(), "The client port from which the connection was made. When connected through a proxy, this will be the port of the proxy."},
        {"user", low_cardinality_string, "Name of the user who initiated the current query."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"address", DataTypeFactory::instance().get("IPv6"), "IP address that was used to make the query. When connected through a proxy and `auth_use_forwarded_address` is set, this will be the address of the client instead of the proxy."},
        {"port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the query. When connected through a proxy and `auth_use_forwarded_address` is set, this will be the port of the client instead of the proxy."},
        {"initial_user", low_cardinality_string, "Name of the user who ran the initial query in the same query chain."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query in the same query chain."},
        {"initial_address", DataTypeFactory::instance().get("IPv6"), "IP address from which the initial query in the same query chain was launched."},
        {"initial_port", std::make_shared<DataTypeUInt16>(), "Client port from which the initial query in the same query chain was launched."},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of the initial query in the same query chain."},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of the initial query in the same query chain, with microsecond precision."},
        {"authenticated_user", low_cardinality_string, "Name of the user who was authenticated in the session."},
        {"interface", std::make_shared<DataTypeUInt8>(), "Interface that the query was initiated from. Possible values: 1 — TCP, 2 — HTTP."},
        {"is_secure", std::make_shared<DataTypeUInt8>(), "The flag which shows whether the connection was secure."},
        {"os_user", low_cardinality_string, "OSs username who runs clickhouse-client."},
        {"client_hostname", low_cardinality_string, "Hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", low_cardinality_string, "The clickhouse-client or another TCP client name."},
        {"client_agent", low_cardinality_string, "The AI coding agent that invoked the client (e.g. `claude-code`, `cursor`), detected from environment variables. Empty if no agent was detected."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "Major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "Minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},
        {"script_query_number", std::make_shared<DataTypeUInt32>(), "A sequential query number in a multi-query script."},
        {"script_line_number", std::make_shared<DataTypeUInt32>(), "A line number in a multi-query script where the current query starts."},
        {"http_method", std::make_shared<DataTypeUInt8>(), "HTTP method that initiated the query. Possible values: 0 - The query was launched from the TCP interface, 1 - GET method was used, 2 - POST method was used, 4 - PUT method was used, 5 - DELETE method was used, 6 - HEAD method was used."},
        {"http_user_agent", low_cardinality_string, "The UserAgent header passed in the HTTP request."},
        {"http_referer", std::make_shared<DataTypeString>(), "HTTP header `Referer` passed in the HTTP query (contains an absolute or partial address of the page making the query)."},
        {"forwarded_for", std::make_shared<DataTypeString>(), "HTTP header `X-Forwarded-For` passed in the HTTP query."},
        {"quota_key", std::make_shared<DataTypeString>(), "The 'quota key' specified in the quotas setting."},
        {"distributed_depth", std::make_shared<DataTypeUInt64>(), "How many times a query was forwarded between servers."},

        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse revision."},

        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "ProfileEvents that measure different metrics for this thread. The description of them could be found in the table system.events."},
    };
}

NamesAndAliases QueryThreadLogElement::getNamesAndAliases()
{
    return
    {
        {"ProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))}, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"}
    };
}

void QueryThreadLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(query_start_time);
    columns[i++]->insert(query_start_time_microseconds);
    columns[i++]->insert(query_duration_ms);

    columns[i++]->insert(read_rows);
    columns[i++]->insert(read_bytes);
    columns[i++]->insert(written_rows);
    columns[i++]->insert(written_bytes);

    columns[i++]->insert(memory_usage);
    columns[i++]->insert(peak_memory_usage);

    auto thread_name_str = toString(thread_name);
    columns[i++]->insertData(thread_name_str.data(), thread_name_str.size());
    columns[i++]->insert(thread_id);
    columns[i++]->insert(master_thread_id);

    columns[i++]->insertData(current_database.data(), current_database.size());
    columns[i++]->insertData(query.data(), query.size());
    columns[i++]->insert(normalized_query_hash);

    QueryLogElement::appendClientInfo(client_info, columns, i);

    columns[i++]->insert(ClickHouseRevision::getVersionRevision());

    if (profile_counters)
    {
        auto * column = columns[i++].get();
        ProfileEvents::dumpToMapColumn(*profile_counters, column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "query_thread_log",
    .description = R"DOCS_MD(
Contains information about threads that execute queries, for example, thread name, thread start time, duration of query processing.

To start logging:

1.  Configure parameters in the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) section.
2.  Set [log_query_threads](/reference/settings/session-settings/log-query#log_query_threads) to 1.

The flushing period of data is set in `flush_interval_milliseconds` parameter of the [query_thread_log](/reference/settings/server-settings/settings/query#query_thread_log) server settings section. To force flushing, use the [SYSTEM FLUSH LOGS](/reference/statements/system#flush-logs) query.

ClickHouse does not delete data from the table automatically. See [Introduction](/reference/system-tables/overview#system-tables-introduction) for more details.

You can use the [log_queries_probability](/reference/settings/session-settings/log-queries#log_queries_probability)) setting to reduce the number of queries, registered in the `query_thread_log` table.
)DOCS_MD",
    .get_columns = QueryThreadLogElement::getColumnsDescription,
    .examples = R"DOCS_MD(
```sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                      clickhouse.eu-central1.internal
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [system.query_log](/reference/system-tables/query_log) — Description of the `query_log` system table which contains common information about queries execution.
- [system.query_views_log](/reference/system-tables/query_views_log) — This table contains information about each view executed during a query.
)DOCS_MD")

}
