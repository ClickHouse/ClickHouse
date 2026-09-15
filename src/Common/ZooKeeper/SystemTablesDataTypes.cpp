#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/SystemTablesDataTypes.h>
#include <DataTypes/DataTypeEnum.h>

namespace Coordination
{

DB::DataTypePtr SystemTablesDataTypes::errorCodeEnum()
{
    static DB::DataTypePtr result = std::make_shared<DB::DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"ZOK",                         static_cast<Int8>(Error::ZOK)},
            {"ZSYSTEMERROR",                static_cast<Int8>(Error::ZSYSTEMERROR)},
            {"ZRUNTIMEINCONSISTENCY",       static_cast<Int8>(Error::ZRUNTIMEINCONSISTENCY)},
            {"ZDATAINCONSISTENCY",          static_cast<Int8>(Error::ZDATAINCONSISTENCY)},
            {"ZCONNECTIONLOSS",             static_cast<Int8>(Error::ZCONNECTIONLOSS)},
            {"ZMARSHALLINGERROR",           static_cast<Int8>(Error::ZMARSHALLINGERROR)},
            {"ZUNIMPLEMENTED",              static_cast<Int8>(Error::ZUNIMPLEMENTED)},
            {"ZOPERATIONTIMEOUT",           static_cast<Int8>(Error::ZOPERATIONTIMEOUT)},
            {"ZBADARGUMENTS",               static_cast<Int8>(Error::ZBADARGUMENTS)},
            {"ZINVALIDSTATE",               static_cast<Int8>(Error::ZINVALIDSTATE)},
            {"ZOUTOFMEMORY",                static_cast<Int8>(Error::ZOUTOFMEMORY)},
            {"ZAPIERROR",                   static_cast<Int8>(Error::ZAPIERROR)},
            {"ZNONODE",                     static_cast<Int8>(Error::ZNONODE)},
            {"ZNOAUTH",                     static_cast<Int8>(Error::ZNOAUTH)},
            {"ZBADVERSION",                 static_cast<Int8>(Error::ZBADVERSION)},
            {"ZNOCHILDRENFOREPHEMERALS",    static_cast<Int8>(Error::ZNOCHILDRENFOREPHEMERALS)},
            {"ZNODEEXISTS",                 static_cast<Int8>(Error::ZNODEEXISTS)},
            {"ZNOTEMPTY",                   static_cast<Int8>(Error::ZNOTEMPTY)},
            {"ZSESSIONEXPIRED",             static_cast<Int8>(Error::ZSESSIONEXPIRED)},
            {"ZINVALIDCALLBACK",            static_cast<Int8>(Error::ZINVALIDCALLBACK)},
            {"ZINVALIDACL",                 static_cast<Int8>(Error::ZINVALIDACL)},
            {"ZAUTHFAILED",                 static_cast<Int8>(Error::ZAUTHFAILED)},
            {"ZCLOSING",                    static_cast<Int8>(Error::ZCLOSING)},
            {"ZNOTHING",                    static_cast<Int8>(Error::ZNOTHING)},
            {"ZSESSIONMOVED",               static_cast<Int8>(Error::ZSESSIONMOVED)},
            {"ZNOTREADONLY",                static_cast<Int8>(Error::ZNOTREADONLY)},
            {"ZNOWATCHER",                  static_cast<Int8>(Error::ZNOWATCHER)},
        });
    return result;
}

DB::DataTypePtr SystemTablesDataTypes::operationEnum()
{
    static DB::DataTypePtr result = std::make_shared<DataTypeEnum16>(
        DataTypeEnum16::Values
        {
            {"Watch",               0},
            {"Close",               static_cast<Int16>(OpNum::Close)},
            {"Error",               static_cast<Int16>(OpNum::Error)},
            {"Create",              static_cast<Int16>(OpNum::Create)},
            {"Remove",              static_cast<Int16>(OpNum::Remove)},
            {"TryRemove",           static_cast<Int16>(OpNum::TryRemove)},
            {"Exists",              static_cast<Int16>(OpNum::Exists)},
            {"Reconfig",            static_cast<Int16>(OpNum::Reconfig)},
            {"Get",                 static_cast<Int16>(OpNum::Get)},
            {"Set",                 static_cast<Int16>(OpNum::Set)},
            {"GetACL",              static_cast<Int16>(OpNum::GetACL)},
            {"SetACL",              static_cast<Int16>(OpNum::SetACL)},
            {"SimpleList",          static_cast<Int16>(OpNum::SimpleList)},
            {"Sync",                static_cast<Int16>(OpNum::Sync)},
            {"Heartbeat",           static_cast<Int16>(OpNum::Heartbeat)},
            {"List",                static_cast<Int16>(OpNum::List)},
            {"Check",               static_cast<Int16>(OpNum::Check)},
            {"Multi",               static_cast<Int16>(OpNum::Multi)},
            {"Create2",             static_cast<Int16>(OpNum::Create2)},
            {"CheckWatch",          static_cast<Int16>(OpNum::CheckWatch)},
            {"RemoveWatch",         static_cast<Int16>(OpNum::RemoveWatch)},
            {"MultiRead",           static_cast<Int16>(OpNum::MultiRead)},
            {"Auth",                static_cast<Int16>(OpNum::Auth)},
            {"SetWatch",            static_cast<Int16>(OpNum::SetWatch)},
            {"SetWatch2",           static_cast<Int16>(OpNum::SetWatch2)},
            {"AddWatch",            static_cast<Int16>(OpNum::AddWatch)},
            {"SessionID",           static_cast<Int16>(OpNum::SessionID)},
            {"FilteredList",        static_cast<Int16>(OpNum::FilteredList)},
            {"CheckNotExists",      static_cast<Int16>(OpNum::CheckNotExists)},
            {"CreateIfNotExists",   static_cast<Int16>(OpNum::CreateIfNotExists)},
            {"RemoveRecursive",     static_cast<Int16>(OpNum::RemoveRecursive)},
            {"CheckStat",           static_cast<Int16>(OpNum::CheckStat)},
            {"FilteredListWithStatsAndData", static_cast<Int16>(OpNum::FilteredListWithStatsAndData)},
        });
    return result;
}

DB::DataTypePtr SystemTablesDataTypes::watchEventTypeEnum()
{
    static DB::DataTypePtr result = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
            {
                {"CREATED",                 static_cast<Int8>(Event::CREATED)},
                {"DELETED",                 static_cast<Int8>(Event::DELETED)},
                {"CHANGED",                 static_cast<Int8>(Event::CHANGED)},
                {"CHILD",                   static_cast<Int8>(Event::CHILD)},
                {"SESSION",                 static_cast<Int8>(Event::SESSION)},
                {"NOTWATCHING",             static_cast<Int8>(Event::NOTWATCHING)},
            });
    return result;
}

DB::DataTypePtr SystemTablesDataTypes::watchStateEnum()
{
    static DB::DataTypePtr result = std::make_shared<DataTypeEnum16>(
        DataTypeEnum16::Values
            {
                {"EXPIRED_SESSION",         static_cast<Int16>(State::EXPIRED_SESSION)},
                {"AUTH_FAILED",             static_cast<Int16>(State::AUTH_FAILED)},
                {"CONNECTING",              static_cast<Int16>(State::CONNECTING)},
                {"ASSOCIATING",             static_cast<Int16>(State::ASSOCIATING)},
                {"CONNECTED",               static_cast<Int16>(State::CONNECTED)},
                {"READONLY",                static_cast<Int16>(State::READONLY)},
                {"NOTCONNECTED",            static_cast<Int16>(State::NOTCONNECTED)},
            });
    return result;
}

}
