#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Interpreters/QueryLog.h>
#include <Common/ClickHouseRevision.h>
#include <Poco/Net/IPAddress.h>
#include <array>


namespace DB
{


Block QueryLogElement::createBlock()
{
    return
    {
        {ColumnUInt8::create(),     std::make_shared<DataTypeUInt8>(),         "type"},
        {ColumnUInt16::create(),     std::make_shared<DataTypeDate>(),         "event_date"},
        {ColumnUInt32::create(),     std::make_shared<DataTypeDateTime>(),     "event_time"},
        {ColumnUInt32::create(),     std::make_shared<DataTypeDateTime>(),     "query_start_time"},
        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "query_duration_ms"},

        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "read_rows"},
        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "read_bytes"},

        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "written_rows"},
        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "written_bytes"},

        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "result_rows"},
        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "result_bytes"},

        {ColumnUInt64::create(),     std::make_shared<DataTypeUInt64>(),     "memory_usage"},

        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "query"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "exception"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "stack_trace"},

        {ColumnUInt8::create(),     std::make_shared<DataTypeUInt8>(),         "is_initial_query"},

        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "user"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "query_id"},
        {ColumnFixedString::create(16), std::make_shared<DataTypeFixedString>(16), "address"},
        {ColumnUInt16::create(),     std::make_shared<DataTypeUInt16>(),     "port"},

        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "initial_user"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "initial_query_id"},
        {ColumnFixedString::create(16), std::make_shared<DataTypeFixedString>(16), "initial_address"},
        {ColumnUInt16::create(),     std::make_shared<DataTypeUInt16>(),     "initial_port"},

        {ColumnUInt8::create(),     std::make_shared<DataTypeUInt8>(),         "interface"},

        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "os_user"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "client_hostname"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "client_name"},
        {ColumnUInt32::create(),     std::make_shared<DataTypeUInt32>(),     "client_revision"},

        {ColumnUInt8::create(),     std::make_shared<DataTypeUInt8>(),         "http_method"},
        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "http_user_agent"},

        {ColumnString::create(),     std::make_shared<DataTypeString>(),     "quota_key"},

        {ColumnUInt32::create(),     std::make_shared<DataTypeUInt32>(),     "revision"},
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
    size_t i = 0;

    block.getByPosition(i++).column->insert(UInt64(type));
    block.getByPosition(i++).column->insert(UInt64(DateLUT::instance().toDayNum(event_time)));
    block.getByPosition(i++).column->insert(UInt64(event_time));
    block.getByPosition(i++).column->insert(UInt64(query_start_time));
    block.getByPosition(i++).column->insert(UInt64(query_duration_ms));

    block.getByPosition(i++).column->insert(UInt64(read_rows));
    block.getByPosition(i++).column->insert(UInt64(read_bytes));

    block.getByPosition(i++).column->insert(UInt64(written_rows));
    block.getByPosition(i++).column->insert(UInt64(written_bytes));

    block.getByPosition(i++).column->insert(UInt64(result_rows));
    block.getByPosition(i++).column->insert(UInt64(result_bytes));

    block.getByPosition(i++).column->insert(UInt64(memory_usage));

    block.getByPosition(i++).column->insertData(query.data(), query.size());
    block.getByPosition(i++).column->insertData(exception.data(), exception.size());
    block.getByPosition(i++).column->insertData(stack_trace.data(), stack_trace.size());

    block.getByPosition(i++).column->insert(UInt64(client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY));

    block.getByPosition(i++).column->insert(client_info.current_user);
    block.getByPosition(i++).column->insert(client_info.current_query_id);
    block.getByPosition(i++).column->insertData(IPv6ToBinary(client_info.current_address.host()).data(), 16);
    block.getByPosition(i++).column->insert(UInt64(client_info.current_address.port()));

    block.getByPosition(i++).column->insert(client_info.initial_user);
    block.getByPosition(i++).column->insert(client_info.initial_query_id);
    block.getByPosition(i++).column->insertData(IPv6ToBinary(client_info.initial_address.host()).data(), 16);
    block.getByPosition(i++).column->insert(UInt64(client_info.initial_address.port()));

    block.getByPosition(i++).column->insert(UInt64(client_info.interface));

    block.getByPosition(i++).column->insert(client_info.os_user);
    block.getByPosition(i++).column->insert(client_info.client_hostname);
    block.getByPosition(i++).column->insert(client_info.client_name);
    block.getByPosition(i++).column->insert(UInt64(client_info.client_revision));

    block.getByPosition(i++).column->insert(UInt64(client_info.http_method));
    block.getByPosition(i++).column->insert(client_info.http_user_agent);

    block.getByPosition(i++).column->insert(client_info.quota_key);

    block.getByPosition(i++).column->insert(UInt64(ClickHouseRevision::get()));
}

}
