#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace Coordination
{

/// Factory of different data types that are needed in ZooKeeper-related system tables
/// (e.g. system.zookeeper_log, system.aggregated_zookeeper_log).
struct SystemTablesDataTypes
{
    static DB::DataTypePtr errorCodeEnum();
    static DB::DataTypePtr operationEnum();
    static DB::DataTypePtr watchEventTypeEnum();
    static DB::DataTypePtr watchStateEnum();
};

}
