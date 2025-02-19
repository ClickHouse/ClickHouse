#pragma once

#include <chrono>
#include <memory>
#include <sys/types.h>

#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndAliases.h>
#include <Core/QueryLogElementType.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>
#include <base/types.h>
#include <Common/ProfileEvents.h>

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
        ThreadStatus * thread_status = nullptr;
        std::atomic_uint64_t elapsed_ms = 0;
        std::chrono::time_point<std::chrono::system_clock> event_time = {};
        ViewStatus event_status = ViewStatus::QUERY_START;

        ViewRuntimeStats(String target_name_, ThreadStatus * thread_status_)
            : target_name(std::move(target_name_))
            , thread_status(thread_status_)
            , event_time(std::chrono::system_clock::now())
            , event_status(ViewStatus::EXCEPTION_BEFORE_START)
        {}

        ViewRuntimeStats() = default;
        ViewRuntimeStats(const ViewRuntimeStats & other)
            : target_name(other.target_name)
            , type(other.type)
            , thread_status(other.thread_status)
            , elapsed_ms(other.elapsed_ms.load())
            , event_time(other.event_time)
            , event_status(other.event_status)
        {}

        ViewRuntimeStats(ViewRuntimeStats && other) noexcept
            : target_name(std::move(other.target_name))
            , type(std::move(other.type))
            , thread_status(std::move(other.thread_status))
            , elapsed_ms(other.elapsed_ms.load())
            , event_time(std::move(other.event_time))
            , event_status(std::move(other.event_status))
        {}

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

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryViewsLog : public SystemLog<QueryViewsLogElement>
{
    using SystemLog<QueryViewsLogElement>::SystemLog;
};

}
