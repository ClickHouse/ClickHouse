#include <Common/QueryIdSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <fmt/format.h>

namespace DB
{

QueryIdSwitcher::QueryIdSwitcher(const std::string & new_query_id)
{
    if (!CurrentThread::isInitialized())
        return;

    prev_query_id = std::string(CurrentThread::getQueryId());

    std::string combined = prev_query_id.empty()
        ? new_query_id
        : fmt::format("{}::{}", prev_query_id, new_query_id);

    current_thread->clearQueryId();
    current_thread->setQueryId(std::move(combined));
    switched = true;
}

QueryIdSwitcher::~QueryIdSwitcher()
{
    if (!switched)
        return;

    current_thread->clearQueryId();
    if (!prev_query_id.empty())
        current_thread->setQueryId(std::move(prev_query_id));
}

}
