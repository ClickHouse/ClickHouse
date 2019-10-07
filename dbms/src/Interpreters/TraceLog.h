#pragma once

#include <Common/QueryProfiler.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/SystemLog.h>

namespace DB
{

struct TraceLogElement
{
    using TimerDataType = DataTypeEnum8;
    static const TimerDataType::Values timer_values;

    time_t event_time{};
    TimerType timer_type{};
    UInt32 thread_number{};
    String query_id{};
    Array trace{};

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
