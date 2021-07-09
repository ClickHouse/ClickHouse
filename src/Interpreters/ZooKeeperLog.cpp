#include <Interpreters/ZooKeeperLog.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/QueryLog.h>
#include <Poco/Net/IPAddress.h>
#include <Common/ClickHouseRevision.h>
#include <Common/IPv6ToBinary.h>

namespace DB
{

NamesAndTypesList ZooKeeperLogElement::getNamesAndTypes()
{
    auto event_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
            {
                {"Send",            static_cast<Int8>(SEND)},
                {"Receive",         static_cast<Int8>(RECEIVE)},
                {"Finalize",        static_cast<Int8>(FINALIZE)},
            });

    return
    {
        {"type", std::move(event_type)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"address", DataTypeFactory::instance().get("IPv6")},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"session_id", std::make_shared<DataTypeInt64>()},

        {"xid", std::make_shared<DataTypeInt32>()},
        {"has_watch", std::make_shared<DataTypeUInt8>()},
        {"op_num", std::make_shared<DataTypeInt32>()},
        {"path", std::make_shared<DataTypeString>()},

        {"data", std::make_shared<DataTypeString>()},

        {"is_ephemeral", std::make_shared<DataTypeUInt8>()},
        {"is_sequential", std::make_shared<DataTypeUInt8>()},

        {"version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())},

        {"requests_size", std::make_shared<DataTypeUInt32>()},
        {"request_idx", std::make_shared<DataTypeUInt32>()},

        {"zxid", std::make_shared<DataTypeInt64>()},
        {"error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())},

        {"watch_type", std::make_shared<DataTypeInt32>()},
        {"watch_state", std::make_shared<DataTypeInt32>()},

        {"path_created", std::make_shared<DataTypeString>()},

        {"stat_czxid", std::make_shared<DataTypeInt64>()},
        {"stat_mzxid", std::make_shared<DataTypeInt64>()},
        {"stat_pzxid", std::make_shared<DataTypeInt64>()},
        {"stat_version", std::make_shared<DataTypeInt32>()},
        {"stat_cversion", std::make_shared<DataTypeInt32>()},
        {"stat_dataLength", std::make_shared<DataTypeInt32>()},
        {"stat_numChildren", std::make_shared<DataTypeInt32>()},

        {"children", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    };
}

void ZooKeeperLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type != UNKNOWN);
    size_t i = 0;

    columns[i++]->insert(type);
    auto event_time_seconds = event_time / 1000000;
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(IPv6ToBinary(address.host()).data());
    columns[i++]->insert(address.port());
    columns[i++]->insert(session_id);

    columns[i++]->insert(xid);
    columns[i++]->insert(has_watch);
    columns[i++]->insert(op_num);
    columns[i++]->insert(path);

    columns[i++]->insert(data);

    columns[i++]->insert(is_ephemeral);
    columns[i++]->insert(is_sequential);

    columns[i++]->insert(version ? Field(*version) : Field());

    columns[i++]->insert(requests_size);
    columns[i++]->insert(request_idx);

    columns[i++]->insert(zxid);
    columns[i++]->insert(error ? Field(*error) : Field());

    columns[i++]->insert(watch_type);
    columns[i++]->insert(watch_state);

    columns[i++]->insert(path_created);

    columns[i++]->insert(stat.czxid);
    columns[i++]->insert(stat.mzxid);
    columns[i++]->insert(stat.pzxid);
    columns[i++]->insert(stat.version);
    columns[i++]->insert(stat.cversion);
    columns[i++]->insert(stat.dataLength);
    columns[i++]->insert(stat.numChildren);

    Array children_array;
    for (const auto & c : children)
        children_array.emplace_back(c);
    columns[i++]->insert(children_array);
}

};
