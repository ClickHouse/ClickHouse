#pragma once

#include <chrono>
#include <memory>
#include <sys/types.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
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

    enum class ViewType : int8_t
    {
        DEFAULT = 1,
        MATERIALIZED = 2,
        LIVE = 3,
        WINDOW = 4,
    };

    struct ViewRuntimeStats
    {
        String target_name;
        ViewType type = ViewType::DEFAULT;
        std::unique_ptr<ThreadStatus> thread_status = nullptr;
        std::atomic_uint64_t elapsed_ms = 0;
        std::chrono::time_point<std::chrono::system_clock> event_time;
        ViewStatus event_status = ViewStatus::QUERY_START;

        void setStatus(ViewStatus s)
        {
            event_status = s;
            event_time = std::chrono::system_clock::now();
        }
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 view_duration_ms{};

    String initial_query_id;
    String view_name;
    UUID view_uuid{UUIDHelpers::Nil};
    ViewType view_type{ViewType::DEFAULT};
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


class QueryViewsLog : public SystemLog<QueryViewsLogElement>
{
    using SystemLog<QueryViewsLogElement>::SystemLog;
};

}
