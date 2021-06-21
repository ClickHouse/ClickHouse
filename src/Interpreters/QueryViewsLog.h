#pragma once

#include <chrono>
#include <memory>
#include <sys/types.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <common/types.h>

namespace ProfileEvents
{
class Counters;
}

namespace DB
{
class ThreadStatus;

struct QueryViewsLogElement
{
    using Status = QueryLogElementType;
    enum class ViewType : int8_t
    {
        DEFAULT,
        MATERIALIZED,
        LIVE
    };

    struct ViewRuntimeStats
    {
        String target_name;
        ViewType type = ViewType::DEFAULT;
        std::shared_ptr<ThreadStatus> thread_status = std::make_shared<ThreadStatus>();
        UInt64 elapsed_ms = 0;
        std::chrono::time_point<std::chrono::system_clock> event_time;
        Status event_status = Status::QUERY_START;

        void setStatus(Status s)
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
    std::shared_ptr<ProfileEvents::Counters> profile_counters;

    Status end_status{EXCEPTION_BEFORE_START};
    Int32 exception_code{};
    String exception;
    String stack_trace;

    static std::string name() { return "QueryViewsLog"; }

    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryViewsLog : public SystemLog<QueryViewsLogElement>
{
    using SystemLog<QueryViewsLogElement>::SystemLog;
};

}
