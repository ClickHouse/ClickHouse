#include "OvercommitTracker.h"

#include <chrono>
#include <mutex>
#include <Interpreters/ProcessList.h>

using namespace std::chrono_literals;

constexpr std::chrono::microseconds ZERO_MICROSEC = 0us;

OvercommitTracker::OvercommitTracker()
    : max_wait_time(ZERO_MICROSEC)
    , picked_tracker(nullptr)
    , cancelation_state(QueryCancelationState::NONE)
{}

void OvercommitTracker::setMaxWaitTime(UInt64 wait_time)
{
    std::lock_guard guard(overcommit_m);
    max_wait_time = wait_time * 1us;
}

bool OvercommitTracker::needToStopQuery(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> lk(overcommit_m);

    if (max_wait_time == ZERO_MICROSEC)
        return true;

    pickQueryToExclude();
    assert(cancelation_state == QueryCancelationState::RUNNING);

    // If no query was chosen we need to stop current query.
    // This may happen if no soft limit is set.
    if (picked_tracker == nullptr)
    {
        cancelation_state = QueryCancelationState::NONE;
        return true;
    }
    if (picked_tracker == tracker)
        return true;
    return !cv.wait_for(lk, max_wait_time, [this]()
    {
        return cancelation_state == QueryCancelationState::NONE;
    });
}

void OvercommitTracker::unsubscribe(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> lk(overcommit_m);
    if (picked_tracker == tracker)
    {
        LOG_DEBUG(getLogger(), "Picked query stopped");

        picked_tracker = nullptr;
        cancelation_state = QueryCancelationState::NONE;
        cv.notify_all();
    }
}

UserOvercommitTracker::UserOvercommitTracker(DB::ProcessListForUser * user_process_list_)
    : user_process_list(user_process_list_)
{}

void UserOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * query_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    // At this moment query list must be read only.
    // BlockQueryIfMemoryLimit is used in ProcessList to guarantee this.
    auto & queries = user_process_list->queries;
    LOG_DEBUG(logger, "Trying to choose query to stop from {} queries", queries.size());
    for (auto const & query : queries)
    {
        if (query.second->isKilled())
            continue;

        auto * memory_tracker = query.second->getMemoryTracker();
        if (!memory_tracker)
            continue;

        auto ratio = memory_tracker->getOvercommitRatio();
        LOG_DEBUG(logger, "Query has ratio {}/{}", ratio.committed, ratio.soft_limit);
        if (ratio.soft_limit != 0 && current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    }
    LOG_DEBUG(logger, "Selected to stop query with overcommit ratio {}/{}",
        current_ratio.committed, current_ratio.soft_limit);
    picked_tracker = query_tracker;
}

void GlobalOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * query_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    process_list->processEachQueryStatus([&](DB::QueryStatus const & query)
    {
        if (query.isKilled())
            return;

        Int64 user_soft_limit = 0;
        if (auto const * user_process_list = query.getUserProcessList())
            user_soft_limit = user_process_list->user_memory_tracker.getSoftLimit();
        if (user_soft_limit == 0)
            return;

        auto * memory_tracker = query.getMemoryTracker();
        if (!memory_tracker)
            return;
        auto ratio = memory_tracker->getOvercommitRatio(user_soft_limit);
        LOG_DEBUG(logger, "Query has ratio {}/{}", ratio.committed, ratio.soft_limit);
        if (current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    });
    LOG_DEBUG(logger, "Selected to stop query with overcommit ratio {}/{}",
        current_ratio.committed, current_ratio.soft_limit);
    picked_tracker = query_tracker;
}
