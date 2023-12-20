#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Common/QueryProfiler.h>
#include <Common/ProfileEvents.h>
#include <Common/TraceSender.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>


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
    /// Allocation size in bytes for TraceType::Memory*
    Int64 size{};
    /// Allocation ptr for TraceType::MemorySample and MemoryProfile.
    UInt64 ptr{};
    /// ProfileEvent for TraceType::ProfileEvent.
    ProfileEvents::Event event{ProfileEvents::end()};
    /// Increment of profile event for TraceType::ProfileEvent.
    ProfileEvents::Count increment{};
    /// How many bytes or events this sample represents.
    Int64 weight{1};
    /// For TraceType::MemoryProfile.
    std::optional<UUID> profile_id;

    static std::string name() { return "TraceLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;

public:
    static const char * getDefaultOrderBy() { return "trace_type, event_date, event_time"; }
};

}
