#include <base/getFQDNOrHostName.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
                {"ZNOTREADONLY",                static_cast<Int8>(Coordination::Error::ZNOTREADONLY)},
            });
}

ColumnsDescription ZooKeeperLogElement::getColumnsDescription()
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
                {"Reconfig",            static_cast<Int16>(Coordination::OpNum::Reconfig)},
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
                {"MultiRead",           static_cast<Int16>(Coordination::OpNum::MultiRead)},
                {"Auth",                static_cast<Int16>(Coordination::OpNum::Auth)},
                {"SessionID",           static_cast<Int16>(Coordination::OpNum::SessionID)},
                {"FilteredList",        static_cast<Int16>(Coordination::OpNum::FilteredList)},
                {"CheckNotExists",      static_cast<Int16>(Coordination::OpNum::CheckNotExists)},
                {"CreateIfNotExists",   static_cast<Int16>(Coordination::OpNum::CreateIfNotExists)},
                {"RemoveRecursive",     static_cast<Int16>(Coordination::OpNum::RemoveRecursive)},
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
                {"READONLY",                static_cast<Int16>(Coordination::State::READONLY)},
                {"NOTCONNECTED",            static_cast<Int16>(Coordination::State::NOTCONNECTED)},
            });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"type", std::move(type_enum), "Event type in the ZooKeeper client. Can have one of the following values: Request — The request has been sent, Response — The response was received, Finalize — The connection is lost, no response was received."},
        {"event_date", std::make_shared<DataTypeDate>(), "The date when the event happened."},
        {"event_time", std::make_shared<DataTypeDateTime64>(6), "The date and time when the event happened."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "The ID of the thread executed this request."},
        {"query_id", std::make_shared<DataTypeString>(), "The ID of a query in scope of which this request was executed."},
        {"address", DataTypeFactory::instance().get("IPv6"), "IP address of ZooKeeper server that was used to make the request."},
        {"port", std::make_shared<DataTypeUInt16>(), "The port of ZooKeeper server that was used to make the request."},
        {"session_id", std::make_shared<DataTypeInt64>(), "The session ID that the ZooKeeper server sets for each connection."},
        {"duration_microseconds", std::make_shared<DataTypeUInt64>(), "The time taken by ZooKeeper to execute the request."},

        {"xid", std::make_shared<DataTypeInt64>(), "The ID of the request within the session. This is usually a sequential request number. It is the same for the request row and the paired response/finalize row."},
        {"has_watch", std::make_shared<DataTypeUInt8>(), "The request whether the watch has been set."},
        {"op_num", op_num_enum, "The type of request or response."},
        {"path", std::make_shared<DataTypeString>(), "The path to the ZooKeeper node specified in the request, or an empty string if the request not requires specifying a path."},

        {"data", std::make_shared<DataTypeString>(), "The data written to the ZooKeeper node (for the SET and CREATE requests — what the request wanted to write, for the response to the GET request — what was read) or an empty string."},

        {"is_ephemeral", std::make_shared<DataTypeUInt8>(), "Is the ZooKeeper node being created as an ephemeral."},
        {"is_sequential", std::make_shared<DataTypeUInt8>(), "Is the ZooKeeper node being created as an sequential."},

        {"version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "The version of the ZooKeeper node that the request expects when executing. This is supported for CHECK, SET, REMOVE requests (is relevant -1 if the request does not check the version or NULL for other requests that do not support version checking)."},

        {"requests_size", std::make_shared<DataTypeUInt32>(), "The number of requests included in the multi request (this is a special request that consists of several consecutive ordinary requests and executes them atomically). All requests included in multi request will have the same xid."},
        {"request_idx", std::make_shared<DataTypeUInt32>(), "The number of the request included in multi request (for multi request — 0, then in order from 1)."},

        {"zxid", std::make_shared<DataTypeInt64>(), "ZooKeeper transaction ID. The serial number issued by the ZooKeeper server in response to a successfully executed request (0 if the request was not executed/returned an error/the client does not know whether the request was executed)."},
        {"error", std::make_shared<DataTypeNullable>(error_enum), "Error code. Can have many values, here are just some of them: ZOK — The request was executed successfully, ZCONNECTIONLOSS — The connection was lost, ZOPERATIONTIMEOUT — The request execution timeout has expired, ZSESSIONEXPIRED — The session has expired, NULL — The request is completed."},

        {"watch_type", std::make_shared<DataTypeNullable>(watch_type_enum), "The type of the watch event (for responses with op_num = Watch), for the remaining responses: NULL."},
        {"watch_state", std::make_shared<DataTypeNullable>(watch_state_enum), "The status of the watch event (for responses with op_num = Watch), for the remaining responses: NULL."},

        {"path_created", std::make_shared<DataTypeString>(), "The path to the created ZooKeeper node (for responses to the CREATE request), may differ from the path if the node is created as a sequential."},

        {"stat_czxid", std::make_shared<DataTypeInt64>(), "The zxid of the change that caused this ZooKeeper node to be created."},
        {"stat_mzxid", std::make_shared<DataTypeInt64>(), "The zxid of the change that last modified this ZooKeeper node."},
        {"stat_pzxid", std::make_shared<DataTypeInt64>(), "The transaction ID of the change that last modified children of this ZooKeeper node."},
        {"stat_version", std::make_shared<DataTypeInt32>(), "The number of changes to the data of this ZooKeeper node."},
        {"stat_cversion", std::make_shared<DataTypeInt32>(), "The number of changes to the children of this ZooKeeper node."},
        {"stat_dataLength", std::make_shared<DataTypeInt32>(), "The length of the data field of this ZooKeeper node."},
        {"stat_numChildren", std::make_shared<DataTypeInt32>(), "The number of children of this ZooKeeper node."},

        {"children", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of child ZooKeeper nodes (for responses to LIST request)."},
    };
}

void ZooKeeperLogElement::appendToBlock(MutableColumns & columns) const
{
    assert(type != UNKNOWN);
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(type);
    auto event_time_seconds = event_time / 1000000;
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(query_id);
    columns[i++]->insertData(IPv6ToBinary(address.host()).data(), 16);
    columns[i++]->insert(address.port());
    columns[i++]->insert(session_id);
    columns[i++]->insert(duration_microseconds);

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

}
