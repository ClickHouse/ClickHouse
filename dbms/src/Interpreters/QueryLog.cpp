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
        {std::make_shared<ColumnUInt8>(),     std::make_shared<DataTypeUInt8>(),         "type"},
        {std::make_shared<ColumnUInt16>(),     std::make_shared<DataTypeDate>(),         "event_date"},
        {std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeDateTime>(),     "event_time"},
        {std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeDateTime>(),     "query_start_time"},
        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "query_duration_ms"},

        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "read_rows"},
        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "read_bytes"},

        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "written_rows"},
        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "written_bytes"},

        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "result_rows"},
        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "result_bytes"},

        {std::make_shared<ColumnUInt64>(),     std::make_shared<DataTypeUInt64>(),     "memory_usage"},

        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "query"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "exception"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "stack_trace"},

        {std::make_shared<ColumnUInt8>(),     std::make_shared<DataTypeUInt8>(),         "is_initial_query"},

        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "user"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "query_id"},
        {std::make_shared<ColumnFixedString>(16), std::make_shared<DataTypeFixedString>(16), "address"},
        {std::make_shared<ColumnUInt16>(),     std::make_shared<DataTypeUInt16>(),     "port"},

        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "initial_user"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "initial_query_id"},
        {std::make_shared<ColumnFixedString>(16), std::make_shared<DataTypeFixedString>(16), "initial_address"},
        {std::make_shared<ColumnUInt16>(),     std::make_shared<DataTypeUInt16>(),     "initial_port"},

        {std::make_shared<ColumnUInt8>(),     std::make_shared<DataTypeUInt8>(),         "interface"},

        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "os_user"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "client_hostname"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "client_name"},
        {std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(),     "client_revision"},

        {std::make_shared<ColumnUInt8>(),     std::make_shared<DataTypeUInt8>(),         "http_method"},
        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "http_user_agent"},

        {std::make_shared<ColumnString>(),     std::make_shared<DataTypeString>(),     "quota_key"},

        {std::make_shared<ColumnUInt32>(),     std::make_shared<DataTypeUInt32>(),     "revision"},
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
