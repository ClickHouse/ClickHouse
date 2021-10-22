#include "OvercommitTracker.h"

#include <Interpreters/ProcessList.h>

bool OvercommitTracker::needToStopQuery(MemoryTracker * tracker)
{
    std::unique_lock<std::mutex> lk(overcommit_m);

    pickQueryToExclude();
    assert(cancelation_state == QueryCancelationState::RUNNING);
    if (tracker == picked_tracker)
        return true;

    auto now = std::chrono::system_clock::now();
    return cv.wait_until(lk, now, [this]()
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
