#pragma once

#include <Interpreters/SystemLog.h>
#include <Common/ZooKeeper/ErrorCounter.h>
#include <Common/LightweightZooKeeperLogger.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

struct LightweightZooKeeperLogElement
{
    /// Identifying a group.
    time_t event_time;
    String path_prefix;
    Coordination::OpNum operation;
    
    /// Group statistics.
    UInt32 count;
    Coordination::ErrorCounter errors;
    UInt64 total_latency_ms;

    static std::string name() { return "LightweightZooKeeperLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class LightweightZooKeeperLog : public SystemLog<LightweightZooKeeperLogElement>
{
    using SystemLog<LightweightZooKeeperLogElement>::SystemLog;
};

}
