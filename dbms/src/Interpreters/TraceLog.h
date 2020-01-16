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
    UInt32 thread_number;
    String query_id;
    Array trace;

    /// for |TraceType::MEMORY|
    Int64 size; /// Allocation size in bytes. In case of deallocation should match the allocation size.
    UInt64 pointer; /// Address of allocated region - to track the deallocations.

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
