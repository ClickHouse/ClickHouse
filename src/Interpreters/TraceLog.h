#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/InstrumentationManager.h>
#include <Interpreters/SystemLog.h>
#include <Common/setThreadName.h>
#include <Common/QueryProfiler.h>
#include <Common/ProfileEvents.h>
#include <Common/TraceSender.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/** Information from sampling profilers.
  */
struct TraceLogElement
{
    bool symbolize = false;

    using TraceDataType = DataTypeEnum8;
    static const TraceDataType::Values trace_values;

    using ContextDataType = DataTypeEnum8;
    static const ContextDataType::Values context_values;

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 timestamp_ns{};
    TraceType trace_type{};
    UInt64 cpu_id{};
    UInt64 thread_id{};
    ThreadName thread_name = ThreadName::UNKNOWN;
    String query_id{};
    /// Runtime addresses, only for symbolization: never log them or write them to a table as is,
    /// `appendToBlock` stores them with `StackTrace::resolveAddressForStorage`.
    std::vector<UInt64> trace{};
    /// Size in bytes for the memory trace types, zero for the others.
    Int64 size{};
    /// Allocation ptr for TraceType::MemorySample. Never log it or write it as is, `appendToBlock` stores a keyed hash of it.
    UInt64 ptr{};
    /// For memory tracing
    std::optional<VariableContext> memory_context{};
    std::optional<VariableContext> memory_blocked_context{};
    /// ProfileEvent for TraceType::ProfileEvent.
    ProfileEvents::Event event{ProfileEvents::end()};
    /// Increment of profile event for TraceType::ProfileEvent.
    ProfileEvents::Count increment{};

    /// Instrumentation values
    UInt64 instrumented_point_id{};
    Int32 function_id = -1;
    String function_name{};
    String handler{};
    std::optional<Instrumentation::EntryType> entry_type{};
    std::optional<UInt64> duration_nanoseconds{};

    static std::string name() { return "TraceLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
public:
    TraceLog(ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<TraceLogElement>> queue_ = nullptr)
        : SystemLog<TraceLogElement>(context_, settings_, queue_),
        symbolize(settings_.symbolize_traces)
    {
    }

    bool symbolize;
};

}
