#include "QueryThreadLog.h"
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/ClickHouseRevision.h>
#include <Poco/Net/IPAddress.h>
#include <array>


namespace DB
{

Block QueryThreadLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeDate>(),          "event_date"},
        {std::make_shared<DataTypeDateTime>(),      "event_time"},
        {std::make_shared<DataTypeDateTime>(),      "query_start_time"},
        {std::make_shared<DataTypeUInt64>(),        "query_duration_ms"},

        {std::make_shared<DataTypeUInt64>(),        "read_rows"},
        {std::make_shared<DataTypeUInt64>(),        "read_bytes"},
        {std::make_shared<DataTypeUInt64>(),        "written_rows"},
        {std::make_shared<DataTypeUInt64>(),        "written_bytes"},
        {std::make_shared<DataTypeInt64>(),         "memory_usage"},
        {std::make_shared<DataTypeInt64>(),         "peak_memory_usage"},

        {std::make_shared<DataTypeString>(),        "thread_name"},
        {std::make_shared<DataTypeUInt32>(),        "thread_number"},
        {std::make_shared<DataTypeInt32>(),         "os_thread_id"},
        {std::make_shared<DataTypeUInt32>(),        "master_thread_number"},
        {std::make_shared<DataTypeInt32>(),         "master_os_thread_id"},
        {std::make_shared<DataTypeString>(),        "query"},

        {std::make_shared<DataTypeUInt8>(),         "is_initial_query"},
        {std::make_shared<DataTypeString>(),        "user"},
        {std::make_shared<DataTypeString>(),        "query_id"},
        {std::make_shared<DataTypeFixedString>(16), "address"},
        {std::make_shared<DataTypeUInt16>(),        "port"},
        {std::make_shared<DataTypeString>(),        "initial_user"},
        {std::make_shared<DataTypeString>(),        "initial_query_id"},
        {std::make_shared<DataTypeFixedString>(16), "initial_address"},
        {std::make_shared<DataTypeUInt16>(),        "initial_port"},
        {std::make_shared<DataTypeUInt8>(),         "interface"},
        {std::make_shared<DataTypeString>(),        "os_user"},
        {std::make_shared<DataTypeString>(),        "client_hostname"},
        {std::make_shared<DataTypeString>(),        "client_name"},
        {std::make_shared<DataTypeUInt32>(),        "client_revision"},
        {std::make_shared<DataTypeUInt32>(),        "client_version_major"},
        {std::make_shared<DataTypeUInt32>(),        "client_version_minor"},
        {std::make_shared<DataTypeUInt32>(),        "client_version_patch"},
        {std::make_shared<DataTypeUInt8>(),         "http_method"},
        {std::make_shared<DataTypeString>(),        "http_user_agent"},
        {std::make_shared<DataTypeString>(),        "quota_key"},

        {std::make_shared<DataTypeUInt32>(),        "revision"},

        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "ProfileEvents.Names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "ProfileEvents.Values"}
    };
}

void QueryThreadLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    columns[i++]->insert(UInt64(event_time));
    columns[i++]->insert(UInt64(query_start_time));
    columns[i++]->insert(UInt64(query_duration_ms));

    columns[i++]->insert(UInt64(read_rows));
    columns[i++]->insert(UInt64(read_bytes));
    columns[i++]->insert(UInt64(written_rows));
    columns[i++]->insert(UInt64(written_bytes));

    columns[i++]->insert(Int64(memory_usage));
    columns[i++]->insert(Int64(peak_memory_usage));

    columns[i++]->insertData(thread_name.data(), thread_name.size());
    columns[i++]->insert(UInt64(thread_number));
    columns[i++]->insert(Int64(os_thread_id));
    columns[i++]->insert(UInt64(master_thread_number));
    columns[i++]->insert(Int64(master_os_thread_id));

    columns[i++]->insertData(query.data(), query.size());

    QueryLogElement::appendClientInfo(client_info, columns, i);

    columns[i++]->insert(UInt64(ClickHouseRevision::get()));

    if (profile_counters)
    {
        auto column_names = columns[i++].get();
        auto column_values = columns[i++].get();
        dumpToArrayColumns(*profile_counters, column_names, column_values, true);
    }
    else
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }
}

}
