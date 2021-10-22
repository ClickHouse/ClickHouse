#include "OvercommitTracker.h"

#include <chrono>
#include <Interpreters/ProcessList.h>

using namespace std::chrono_literals;

OvercommitTracker::OvercommitTracker()
    : max_wait_time(0us)
    , picked_tracker(nullptr)
    , cancelation_state(QueryCancelationState::NONE)
{}

void OvercommitTracker::setMaxWaitTime(UInt64 wait_time)
{
    max_wait_time = wait_time * 1us;
}

bool OvercommitTracker::needToStopQuery(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> lk(overcommit_m);

    pickQueryToExclude();
    assert(cancelation_state == QueryCancelationState::RUNNING);
    if (tracker == picked_tracker)
        return true;

    return cv.wait_for(lk, max_wait_time, [this]()
    {
        return cancelation_state == QueryCancelationState::NONE;
    });
}

UserOvercommitTracker::UserOvercommitTracker(DB::ProcessListForUser * user_process_list_)
    : user_process_list(user_process_list_)
{}

void UserOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * current_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    //TODO: ensure this container is not being modified
    for (auto const & query : user_process_list->queries)
    {
        auto * memory_tracker = query.second->getMemoryTracker();
        auto ratio = memory_tracker->getOvercommitRatio();
        if (current_ratio < ratio)
        {
            current_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    }
    assert(current_tracker != nullptr);
    picked_tracker = current_tracker;
}

void GlobalOvercommitTracker::pickQueryToExcludeImpl()
{
    MemoryTracker * current_tracker = nullptr;
    OvercommitRatio current_ratio{0, 0};
    process_list->processEachQueryStatus([&](DB::QueryStatus const & query)
    {
        auto * memory_tracker = query.getMemoryTracker();
        auto ratio = memory_tracker->getOvercommitRatio();
        if (current_ratio < ratio)
        {
            current_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    });
    assert(current_tracker != nullptr);
    picked_tracker = current_tracker;
}
