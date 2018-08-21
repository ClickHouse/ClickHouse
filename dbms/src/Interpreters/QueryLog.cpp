#include <Common/ProfileEvents.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
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


Block QueryLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeUInt8>(),                                   "type"},
        {std::make_shared<DataTypeDate>(),                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                "event_time"},
        {std::make_shared<DataTypeDateTime>(),                                "query_start_time"},
        {std::make_shared<DataTypeUInt64>(),                                  "query_duration_ms"},

        {std::make_shared<DataTypeUInt64>(),                                  "read_rows"},
        {std::make_shared<DataTypeUInt64>(),                                  "read_bytes"},
        {std::make_shared<DataTypeUInt64>(),                                  "written_rows"},
        {std::make_shared<DataTypeUInt64>(),                                  "written_bytes"},
        {std::make_shared<DataTypeUInt64>(),                                  "result_rows"},
        {std::make_shared<DataTypeUInt64>(),                                  "result_bytes"},
        {std::make_shared<DataTypeUInt64>(),                                  "memory_usage"},

        {std::make_shared<DataTypeString>(),                                  "query"},
        {std::make_shared<DataTypeString>(),                                  "exception"},
        {std::make_shared<DataTypeString>(),                                  "stack_trace"},

        {std::make_shared<DataTypeUInt8>(),                                   "is_initial_query"},
        {std::make_shared<DataTypeString>(),                                  "user"},
        {std::make_shared<DataTypeString>(),                                  "query_id"},
        {std::make_shared<DataTypeFixedString>(16),                           "address"},
        {std::make_shared<DataTypeUInt16>(),                                  "port"},
        {std::make_shared<DataTypeString>(),                                  "initial_user"},
        {std::make_shared<DataTypeString>(),                                  "initial_query_id"},
        {std::make_shared<DataTypeFixedString>(16),                           "initial_address"},
        {std::make_shared<DataTypeUInt16>(),                                  "initial_port"},
        {std::make_shared<DataTypeUInt8>(),                                   "interface"},
        {std::make_shared<DataTypeString>(),                                  "os_user"},
        {std::make_shared<DataTypeString>(),                                  "client_hostname"},
        {std::make_shared<DataTypeString>(),                                  "client_name"},
        {std::make_shared<DataTypeUInt32>(),                                  "client_revision"},
        {std::make_shared<DataTypeUInt32>(),                                  "client_version_major"},
        {std::make_shared<DataTypeUInt32>(),                                  "client_version_minor"},
        {std::make_shared<DataTypeUInt32>(),                                  "client_version_patch"},
        {std::make_shared<DataTypeUInt8>(),                                   "http_method"},
        {std::make_shared<DataTypeString>(),                                  "http_user_agent"},
        {std::make_shared<DataTypeString>(),                                  "quota_key"},

        {std::make_shared<DataTypeUInt32>(),                                  "revision"},

        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), "thread_numbers"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "ProfileEvents.Names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "ProfileEvents.Values"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Settings.Names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Settings.Values"}
    };
}


static std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address)
{
    std::array<char, 16> res;

    if (Poco::Net::IPAddress::IPv6 == address.family())
    {
        memcpy(res.data(), address.addr(), 16);
    }
    else if (Poco::Net::IPAddress::IPv4 == address.family())
    {
        /// Convert to IPv6-mapped address.
        memset(res.data(), 0, 10);
        res[10] = '\xFF';
        res[11] = '\xFF';
        memcpy(&res[12], address.addr(), 4);
    }
    else
        memset(res.data(), 0, 16);

    return res;
}


void QueryLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(UInt64(type));
    columns[i++]->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    columns[i++]->insert(UInt64(event_time));
    columns[i++]->insert(UInt64(query_start_time));
    columns[i++]->insert(UInt64(query_duration_ms));

    columns[i++]->insert(UInt64(read_rows));
    columns[i++]->insert(UInt64(read_bytes));
    columns[i++]->insert(UInt64(written_rows));
    columns[i++]->insert(UInt64(written_bytes));
    columns[i++]->insert(UInt64(result_rows));
    columns[i++]->insert(UInt64(result_bytes));

    columns[i++]->insert(UInt64(memory_usage));

    columns[i++]->insertData(query.data(), query.size());
    columns[i++]->insertData(exception.data(), exception.size());
    columns[i++]->insertData(stack_trace.data(), stack_trace.size());

    appendClientInfo(client_info, columns, i);

    columns[i++]->insert(UInt64(ClickHouseRevision::get()));

    {
        Array threads_array;
        threads_array.reserve(thread_numbers.size());
        for (const UInt32 thread_number : thread_numbers)
            threads_array.emplace_back(UInt64(thread_number));
        columns[i++]->insert(threads_array);
    }

    if (profile_counters)
    {
        auto column_names = columns[i++].get();
        auto column_values = columns[i++].get();
        ProfileEvents::dumpToArrayColumns(*profile_counters, column_names, column_values, true);
    }
    else
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }

    if (query_settings)
    {
        auto column_names = columns[i++].get();
        auto column_values = columns[i++].get();
        query_settings->dumpToArrayColumns(column_names, column_values, true);
    }
    else
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }

    block.setColumns(std::move(columns));
}

void QueryLogElement::appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i)
{
    columns[i++]->insert(UInt64(client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY));

    columns[i++]->insert(client_info.current_user);
    columns[i++]->insert(client_info.current_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.current_address.host()).data(), 16);
    columns[i++]->insert(UInt64(client_info.current_address.port()));

    columns[i++]->insert(client_info.initial_user);
    columns[i++]->insert(client_info.initial_query_id);
    columns[i++]->insertData(IPv6ToBinary(client_info.initial_address.host()).data(), 16);
    columns[i++]->insert(UInt64(client_info.initial_address.port()));

    columns[i++]->insert(UInt64(client_info.interface));

    columns[i++]->insert(client_info.os_user);
    columns[i++]->insert(client_info.client_hostname);
    columns[i++]->insert(client_info.client_name);
    columns[i++]->insert(UInt64(client_info.client_revision));
    columns[i++]->insert(UInt64(client_info.client_version_major));
    columns[i++]->insert(UInt64(client_info.client_version_minor));
    columns[i++]->insert(UInt64(client_info.client_version_patch));

    columns[i++]->insert(UInt64(client_info.http_method));
    columns[i++]->insert(client_info.http_user_agent);

    columns[i++]->insert(client_info.quota_key);
}
}
