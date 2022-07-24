#include "OvercommitTracker.h"

#include <chrono>
#include <mutex>
#include <Interpreters/ProcessList.h>

using namespace std::chrono_literals;

constexpr std::chrono::microseconds ZERO_MICROSEC = 0us;

OvercommitTracker::OvercommitTracker(std::mutex & global_mutex_)
    : max_wait_time(ZERO_MICROSEC)
    , picked_tracker(nullptr)
    , cancelation_state(QueryCancelationState::NONE)
    , global_mutex(global_mutex_)
{}

void OvercommitTracker::setMaxWaitTime(UInt64 wait_time)
{
    std::lock_guard guard(overcommit_m);
    max_wait_time = wait_time * 1us;
}

bool OvercommitTracker::needToStopQuery(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> global_lock(global_mutex);
    std::unique_lock<std::mutex> lk(overcommit_m);

    if (max_wait_time == ZERO_MICROSEC)
        return true;

    pickQueryToExclude();
    assert(cancelation_state == QueryCancelationState::RUNNING);
    global_lock.unlock();

    // If no query was chosen we need to stop current query.
    // This may happen if no soft limit is set.
    if (picked_tracker == nullptr)
    {
        cancelation_state = QueryCancelationState::NONE;
        return true;
    }
    if (picked_tracker == tracker)
        return true;
    bool timeout = !cv.wait_for(lk, max_wait_time, [this]()
    {
        return cancelation_state == QueryCancelationState::NONE;
    });

    return timeout;
}

void OvercommitTracker::unsubscribe(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> lk(overcommit_m);
    if (picked_tracker == tracker)
    {
        picked_tracker = nullptr;
        cancelation_state = QueryCancelationState::NONE;
        cv.notify_all();
    }
}

UserOvercommitTracker::UserOvercommitTracker(DB::ProcessList * process_list, DB::ProcessListForUser * user_process_list_)
    : OvercommitTracker(process_list->mutex)
    , user_process_list(user_process_list_)
{}

void UserOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * query_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    // At this moment query list must be read only.
    // BlockQueryIfMemoryLimit is used in ProcessList to guarantee this.
    auto & queries = user_process_list->queries;
    for (auto const & query : queries)
    {
        if (query.second->isKilled())
            continue;

        auto * memory_tracker = query.second->getMemoryTracker();
        if (!memory_tracker)
            continue;

        auto ratio = memory_tracker->getOvercommitRatio();
        if (ratio.soft_limit != 0 && current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    }
    picked_tracker = query_tracker;
}

GlobalOvercommitTracker::GlobalOvercommitTracker(DB::ProcessList * process_list_)
    : OvercommitTracker(process_list_->mutex)
    , process_list(process_list_)
{}

void GlobalOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * query_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    // At this moment query list must be read only.
    // BlockQueryIfMemoryLimit is used in ProcessList to guarantee this.
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
        if (current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    });
    picked_tracker = query_tracker;
}
