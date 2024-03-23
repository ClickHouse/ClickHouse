#include "QueryThreadLog.h"
#include <array>
#include <base/getFQDNOrHostName.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
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
#include <Poco/Net/IPAddress.h>
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
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>(), "The hash of normalized query - with wiped constanstans, etc."},

        {"is_initial_query", std::make_shared<DataTypeUInt8>(), "Query type. Possible values: 1 — Query was initiated by the client, 0 — Query was initiated by another query for distributed query execution."},
        {"user", low_cardinality_string, "Name of the user who initiated the current query."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"address", DataTypeFactory::instance().get("IPv6"), "IP address that was used to make the query."},
        {"port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the query."},
        {"initial_user", low_cardinality_string, "Name of the user who ran the initial query (for distributed query execution)."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "ID of the initial query (for distributed query execution)."},
        {"initial_address", DataTypeFactory::instance().get("IPv6"), "IP address that the parent query was launched from."},
        {"initial_port", std::make_shared<DataTypeUInt16>(), "The client port that was used to make the parent query."},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>(), "Start time of the initial query execution."},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Start time of the initial query execution "},
        {"interface", std::make_shared<DataTypeUInt8>(), "Interface that the query was initiated from. Possible values: 1 — TCP, 2 — HTTP."},
        {"is_secure", std::make_shared<DataTypeUInt8>(), "The flag which shows whether the connection was secure."},
        {"os_user", low_cardinality_string, "OSs username who runs clickhouse-client."},
        {"client_hostname", low_cardinality_string, "Hostname of the client machine where the clickhouse-client or another TCP client is run."},
        {"client_name", low_cardinality_string, "The clickhouse-client or another TCP client name."},
        {"client_revision", std::make_shared<DataTypeUInt32>(), "Revision of the clickhouse-client or another TCP client."},
        {"client_version_major", std::make_shared<DataTypeUInt32>(), "Major version of the clickhouse-client or another TCP client."},
        {"client_version_minor", std::make_shared<DataTypeUInt32>(), "Minor version of the clickhouse-client or another TCP client."},
        {"client_version_patch", std::make_shared<DataTypeUInt32>(), "Patch component of the clickhouse-client or another TCP client version."},
        {"http_method", std::make_shared<DataTypeUInt8>(), "HTTP method that initiated the query. Possible values: 0 — The query was launched from the TCP interface, 1 — GET method was used., 2 — POST method was used."},
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

    columns[i++]->insertData(thread_name.data(), thread_name.size());
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
