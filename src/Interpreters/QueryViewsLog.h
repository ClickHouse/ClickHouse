#pragma once

#include <chrono>
#include <memory>
#include <sys/types.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums_fwd.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/QueryViewRuntimeStats.h>
#include <base/types.h>

namespace ProfileEvents
{
class Counters;
}

namespace DB
{
class ThreadStatus;

struct QueryViewsLogElement
{
    using ViewStatus = QueryLogElementType;

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 view_duration_ms{};

    String initial_query_id;
    String view_name;
    UUID view_uuid{UUIDHelpers::Nil};
    QueryViewType view_type{QueryViewType::DEFAULT};
    String view_query;
    String view_target;

    UInt64 read_rows{};
    UInt64 read_bytes{};
    UInt64 written_rows{};
    UInt64 written_bytes{};
    Int64 peak_memory_usage{};
    std::shared_ptr<ProfileEvents::Counters::Snapshot> profile_counters;

    ViewStatus status = ViewStatus::QUERY_START;
    Int32 exception_code{};
    String exception;
    String stack_trace;

    static std::string name() { return "QueryLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

}
