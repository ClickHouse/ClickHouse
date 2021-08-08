#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Common/QueryProfiler.h>
#include <Common/TraceCollector.h>


namespace DB
{

/** Information from sampling profilers.
  */
struct TraceLogElement
{
    using TraceDataType = DataTypeEnum8;
    static const TraceDataType::Values trace_values;

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 timestamp_ns{};
    TraceType trace_type{};
    UInt64 thread_id{};
    String query_id{};
    Array trace{};
    Int64 size{}; /// Allocation size in bytes for TraceType::Memory

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
