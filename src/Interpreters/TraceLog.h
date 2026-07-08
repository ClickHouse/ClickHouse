#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Common/QueryProfiler.h>
#include <Common/ProfileEvents.h>
#include <Common/TraceSender.h>
#include <Core/NamesAndTypes.h>
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

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 timestamp_ns{};
    TraceType trace_type{};
    UInt64 thread_id{};
    String query_id{};
    std::vector<UInt64> trace{};
    /// Allocation size in bytes for TraceType::Memory and TraceType::MemorySample.
    Int64 size{};
    /// Allocation ptr for TraceType::MemorySample.
    UInt64 ptr{};
    /// ProfileEvent for TraceType::ProfileEvent.
    ProfileEvents::Event event{ProfileEvents::end()};
    /// Increment of profile event for TraceType::ProfileEvent.
    ProfileEvents::Count increment{};

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
