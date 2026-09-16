#include <Common/QueryIdSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTrackerSwitcher.h>

#include <fmt/format.h>

namespace DB
{

QueryIdSwitcher::QueryIdSwitcher(const std::string & new_query_id)
{
    if (!CurrentThread::isInitialized())
        return;

    MemoryTrackerSwitcher query_id_memory_scope(&total_memory_tracker);
    std::string previous_query_id(CurrentThread::getQueryId());

    std::string combined = previous_query_id.empty()
        ? new_query_id
        : fmt::format("{}::{}", previous_query_id, new_query_id);

    current_thread->clearQueryId();
    current_thread->setQueryId(std::move(combined));
    prev_query_id = std::move(previous_query_id);
    switched = true;
}

QueryIdSwitcher::~QueryIdSwitcher()
{
    if (!switched)
        return;

    MemoryTrackerSwitcher query_id_memory_scope(&total_memory_tracker);
    current_thread->clearQueryId();
    if (!prev_query_id.empty())
        current_thread->setQueryId(std::move(prev_query_id));
}

}
