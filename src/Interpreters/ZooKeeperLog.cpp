#include <Interpreters/ZooKeeperLog.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/QueryLog.h>
#include <Poco/Net/IPAddress.h>
#include <Common/IPv6ToBinary.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace DB
{


DataTypePtr getCoordinationErrorCodesEnumType()
{
    return std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
            {
                {"ZOK",                         static_cast<Int8>(Coordination::Error::ZOK)},

                {"ZSYSTEMERROR",                static_cast<Int8>(Coordination::Error::ZSYSTEMERROR)},
                {"ZRUNTIMEINCONSISTENCY",       static_cast<Int8>(Coordination::Error::ZRUNTIMEINCONSISTENCY)},
                {"ZDATAINCONSISTENCY",          static_cast<Int8>(Coordination::Error::ZDATAINCONSISTENCY)},
                {"ZCONNECTIONLOSS",             static_cast<Int8>(Coordination::Error::ZCONNECTIONLOSS)},
                {"ZMARSHALLINGERROR",           static_cast<Int8>(Coordination::Error::ZMARSHALLINGERROR)},
                {"ZUNIMPLEMENTED",              static_cast<Int8>(Coordination::Error::ZUNIMPLEMENTED)},
                {"ZOPERATIONTIMEOUT",           static_cast<Int8>(Coordination::Error::ZOPERATIONTIMEOUT)},
                {"ZBADARGUMENTS",               static_cast<Int8>(Coordination::Error::ZBADARGUMENTS)},
                {"ZINVALIDSTATE",               static_cast<Int8>(Coordination::Error::ZINVALIDSTATE)},

                {"ZAPIERROR",                   static_cast<Int8>(Coordination::Error::ZAPIERROR)},
                {"ZNONODE",                     static_cast<Int8>(Coordination::Error::ZNONODE)},
                {"ZNOAUTH",                     static_cast<Int8>(Coordination::Error::ZNOAUTH)},
                {"ZBADVERSION",                 static_cast<Int8>(Coordination::Error::ZBADVERSION)},
                {"ZNOCHILDRENFOREPHEMERALS",    static_cast<Int8>(Coordination::Error::ZNOCHILDRENFOREPHEMERALS)},
                {"ZNODEEXISTS",                 static_cast<Int8>(Coordination::Error::ZNODEEXISTS)},
                {"ZNOTEMPTY",                   static_cast<Int8>(Coordination::Error::ZNOTEMPTY)},
                {"ZSESSIONEXPIRED",             static_cast<Int8>(Coordination::Error::ZSESSIONEXPIRED)},
                {"ZINVALIDCALLBACK",            static_cast<Int8>(Coordination::Error::ZINVALIDCALLBACK)},
                {"ZINVALIDACL",                 static_cast<Int8>(Coordination::Error::ZINVALIDACL)},
                {"ZAUTHFAILED",                 static_cast<Int8>(Coordination::Error::ZAUTHFAILED)},
                {"ZCLOSING",                    static_cast<Int8>(Coordination::Error::ZCLOSING)},
                {"ZNOTHING",                    static_cast<Int8>(Coordination::Error::ZNOTHING)},
                {"ZSESSIONMOVED",               static_cast<Int8>(Coordination::Error::ZSESSIONMOVED)},
            });
}

NamesAndTypesList ZooKeeperLogElement::getNamesAndTypes()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
            {
                {"Request",         static_cast<Int8>(REQUEST)},
                {"Response",        static_cast<Int8>(RESPONSE)},
                {"Finalize",        static_cast<Int8>(FINALIZE)},
            });

    auto op_num_enum = std::make_shared<DataTypeEnum16>(
        DataTypeEnum16::Values
            {
                {"Watch",               0},
                {"Close",               static_cast<Int16>(Coordination::OpNum::Close)},
                {"Error",               static_cast<Int16>(Coordination::OpNum::Error)},
                {"Create",              static_cast<Int16>(Coordination::OpNum::Create)},
                {"Remove",              static_cast<Int16>(Coordination::OpNum::Remove)},
                {"Exists",              static_cast<Int16>(Coordination::OpNum::Exists)},
                {"Get",                 static_cast<Int16>(Coordination::OpNum::Get)},
                {"Set",                 static_cast<Int16>(Coordination::OpNum::Set)},
                {"GetACL",              static_cast<Int16>(Coordination::OpNum::GetACL)},
                {"SetACL",              static_cast<Int16>(Coordination::OpNum::SetACL)},
                {"SimpleList",          static_cast<Int16>(Coordination::OpNum::SimpleList)},
                {"Sync",                static_cast<Int16>(Coordination::OpNum::Sync)},
                {"Heartbeat",           static_cast<Int16>(Coordination::OpNum::Heartbeat)},
                {"List",                static_cast<Int16>(Coordination::OpNum::List)},
                {"Check",               static_cast<Int16>(Coordination::OpNum::Check)},
                {"Multi",               static_cast<Int16>(Coordination::OpNum::Multi)},
                {"Auth",                static_cast<Int16>(Coordination::OpNum::Auth)},
                {"SessionID",           static_cast<Int16>(Coordination::OpNum::SessionID)},
            });

    auto error_enum = getCoordinationErrorCodesEnumType();

    auto watch_type_enum = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
            {
                {"CREATED",                 static_cast<Int8>(Coordination::Event::CREATED)},
                {"DELETED",                 static_cast<Int8>(Coordination::Event::DELETED)},
                {"CHANGED",                 static_cast<Int8>(Coordination::Event::CHANGED)},
                {"CHILD",                   static_cast<Int8>(Coordination::Event::CHILD)},
                {"SESSION",                 static_cast<Int8>(Coordination::Event::SESSION)},
                {"NOTWATCHING",             static_cast<Int8>(Coordination::Event::NOTWATCHING)},
            });

    auto watch_state_enum = std::make_shared<DataTypeEnum16>(
        DataTypeEnum16::Values
            {
                {"EXPIRED_SESSION",         static_cast<Int16>(Coordination::State::EXPIRED_SESSION)},
                {"AUTH_FAILED",             static_cast<Int16>(Coordination::State::AUTH_FAILED)},
                {"CONNECTING",              static_cast<Int16>(Coordination::State::CONNECTING)},
                {"ASSOCIATING",             static_cast<Int16>(Coordination::State::ASSOCIATING)},
                {"CONNECTED",               static_cast<Int16>(Coordination::State::CONNECTED)},
                {"NOTCONNECTED",            static_cast<Int16>(Coordination::State::NOTCONNECTED)},
            });

    return
    {
        {"type", std::move(type_enum)},
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime64>(6)},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"address", DataTypeFactory::instance().get("IPv6")},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"session_id", std::make_shared<DataTypeInt64>()},

        {"xid", std::make_shared<DataTypeInt32>()},
        {"has_watch", std::make_shared<DataTypeUInt8>()},
        {"op_num", op_num_enum},
        {"path", std::make_shared<DataTypeString>()},

        {"data", std::make_shared<DataTypeString>()},

        {"is_ephemeral", std::make_shared<DataTypeUInt8>()},
        {"is_sequential", std::make_shared<DataTypeUInt8>()},

        {"version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())},

        {"requests_size", std::make_shared<DataTypeUInt32>()},
        {"request_idx", std::make_shared<DataTypeUInt32>()},

        {"zxid", std::make_shared<DataTypeInt64>()},
        {"error", std::make_shared<DataTypeNullable>(error_enum)},

        {"watch_type", std::make_shared<DataTypeNullable>(watch_type_enum)},
        {"watch_state", std::make_shared<DataTypeNullable>(watch_state_enum)},

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
    columns[i++]->insert(thread_id);
    columns[i++]->insert(query_id);
    columns[i++]->insertData(IPv6ToBinary(address.host()).data(), 16);
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

    columns[i++]->insert(watch_type ? Field(*watch_type) : Field());
    columns[i++]->insert(watch_state ? Field(*watch_state) : Field());

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
