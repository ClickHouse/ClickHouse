#pragma once

#include <Common/QueryProfiler.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/SystemLog.h>

namespace DB
{

struct TraceLogElement
{
    using TraceDataType = DataTypeEnum8;
    static const TraceDataType::Values trace_values;

    time_t event_time;
    TraceType trace_type;
    UInt32 thread_number;
    String query_id;
    Array trace;

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
