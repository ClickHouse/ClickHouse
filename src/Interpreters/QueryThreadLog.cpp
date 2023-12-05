#include "QueryThreadLog.h"
#include <array>
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

NamesAndTypesList QueryThreadLogElement::getNamesAndTypes()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"query_start_time", std::make_shared<DataTypeDateTime>()},
        {"query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"query_duration_ms", std::make_shared<DataTypeUInt64>()},

        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"written_rows", std::make_shared<DataTypeUInt64>()},
        {"written_bytes", std::make_shared<DataTypeUInt64>()},
        {"memory_usage", std::make_shared<DataTypeInt64>()},
        {"peak_memory_usage", std::make_shared<DataTypeInt64>()},

        {"thread_name", low_cardinality_string},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"master_thread_id", std::make_shared<DataTypeUInt64>()},
        {"current_database", low_cardinality_string},
        {"query", std::make_shared<DataTypeString>()},
        {"normalized_query_hash", std::make_shared<DataTypeUInt64>()},

        {"is_initial_query", std::make_shared<DataTypeUInt8>()},
        {"user", low_cardinality_string},
        {"query_id", std::make_shared<DataTypeString>()},
        {"address", DataTypeFactory::instance().get("IPv6")},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"initial_user", low_cardinality_string},
        {"initial_query_id", std::make_shared<DataTypeString>()},
        {"initial_address", DataTypeFactory::instance().get("IPv6")},
        {"initial_port", std::make_shared<DataTypeUInt16>()},
        {"initial_query_start_time", std::make_shared<DataTypeDateTime>()},
        {"initial_query_start_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"interface", std::make_shared<DataTypeUInt8>()},
        {"is_secure", std::make_shared<DataTypeUInt8>()},
        {"os_user", low_cardinality_string},
        {"client_hostname", low_cardinality_string},
        {"client_name", low_cardinality_string},
        {"client_revision", std::make_shared<DataTypeUInt32>()},
        {"client_version_major", std::make_shared<DataTypeUInt32>()},
        {"client_version_minor", std::make_shared<DataTypeUInt32>()},
        {"client_version_patch", std::make_shared<DataTypeUInt32>()},
        {"http_method", std::make_shared<DataTypeUInt8>()},
        {"http_user_agent", low_cardinality_string},
        {"http_referer", std::make_shared<DataTypeString>()},
        {"forwarded_for", std::make_shared<DataTypeString>()},
        {"quota_key", std::make_shared<DataTypeString>()},
        {"distributed_depth", std::make_shared<DataTypeUInt64>()},

        {"revision", std::make_shared<DataTypeUInt32>()},

        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>())},
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
