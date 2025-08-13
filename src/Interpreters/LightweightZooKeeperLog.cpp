#include <Interpreters/LightweightZooKeeperLog.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Columns/ColumnMap.h>
#include <base/getFQDNOrHostName.h>

namespace DB
{

DataTypePtr getOperationEnumType()
{
    return std::make_shared<DataTypeEnum16>(
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
}

DataTypePtr getErrorEnumType()
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

ColumnsDescription LightweightZooKeeperLogElement::getColumnsDescription()
{
    ColumnsDescription result;
    ParserCodec codec_parser;

    result.add({"hostname",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Hostname of the server."});

    result.add({"event_date",
                std::make_shared<DataTypeDate>(),
                parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Date the group was flushed."});

    result.add({"event_time",
                std::make_shared<DataTypeDateTime>(),
                parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Time the group was flushed."});

    result.add({"session_id",
                std::make_shared<DataTypeInt64>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Session id."});

    result.add({"parent_path",
                std::make_shared<DataTypeString>(),
                parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Prefix of the path."});

    result.add({"operation",
                getOperationEnumType(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Type of ZooKeeper operation."});

    result.add({"count",
                std::make_shared<DataTypeUInt32>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Number of operations in the (session_id, parent_path, operation) group."});

    result.add({"errors",
                std::make_shared<DataTypeMap>(getErrorEnumType(), std::make_shared<DataTypeUInt32>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Errors in the (session_id, parent_path, operation) group."});

    result.add({"average_latency",
                std::make_shared<DataTypeFloat64>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Average latency across all operations in (session_id, parent_path, operation) group, in microseconds."});
    return result;
}

void LightweightZooKeeperLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(session_id);
    columns[i++]->insert(parent_path);
    columns[i++]->insert(operation);
    columns[i++]->insert(count);
    errors->dumpToMapColumn(&typeid_cast<DB::ColumnMap &>(*columns[i++]));
    columns[i++]->insert(static_cast<Float64>(total_latency_microseconds) / count);
}

}
