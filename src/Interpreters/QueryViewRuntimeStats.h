#pragma once

#include <chrono>
#include <memory>
#include <sys/types.h>

#include <base/types.h>
#include <Common/ThreadStatus.h>


namespace DB
{

enum class QueryViewType : int8_t
{
    DEFAULT = 1,
    MATERIALIZED = 2,
    LIVE = 3
};

struct QueryViewRuntimeStats
{
    String target_name;
    QueryViewType type = QueryViewType::DEFAULT;
    std::unique_ptr<ThreadStatus> thread_status = nullptr;
    std::atomic_uint64_t elapsed_ms = 0;
    std::chrono::time_point<std::chrono::system_clock> event_time;
    QueryLogElementType event_status = QueryLogElementType::QUERY_START;

    void setStatus(QueryLogElementType s)
    {
        event_status = s;
        event_time = std::chrono::system_clock::now();
    }
};

}
