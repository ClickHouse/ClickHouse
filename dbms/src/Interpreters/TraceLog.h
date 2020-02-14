#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Common/QueryProfiler.h>
#include <Common/TraceCollector.h>

namespace DB
{

struct TraceLogElement
{
    using TraceDataType = DataTypeEnum8;
    static const TraceDataType::Values trace_values;

    time_t event_time;
    TraceType trace_type;
    UInt64 thread_id;
    String query_id;
    Array trace;

    UInt64 size; /// Allocation size in bytes for |TraceType::MEMORY|

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
