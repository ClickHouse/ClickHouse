#include "OvercommitTracker.h"

#include <chrono>
#include <mutex>
#include <Common/ProfileEvents.h>
#include <Interpreters/ProcessList.h>

namespace ProfileEvents
{
    extern const Event MemoryOvercommitWaitTimeMicroseconds;
}

using namespace std::chrono_literals;

constexpr std::chrono::microseconds ZERO_MICROSEC = 0us;

OvercommitTracker::OvercommitTracker(std::mutex & global_mutex_)
    : picked_tracker(nullptr)
    , cancellation_state(QueryCancellationState::NONE)
    , global_mutex(global_mutex_)
    , freed_memory(0)
    , required_memory(0)
    , next_id(0)
    , id_to_release(0)
    , allow_release(true)
{}

#define LOG_DEBUG_SAFE(...)                                                                               \
    do {                                                                                                  \
        OvercommitTrackerBlockerInThread blocker;                                                         \
        try                                                                                               \
        {                                                                                                 \
            ALLOW_ALLOCATIONS_IN_SCOPE;                                                                   \
            LOG_DEBUG(__VA_ARGS__);                                                                       \
        }                                                                                                 \
        catch (...)                                                                                       \
        {                                                                                                 \
            if (fprintf(stderr, "Allocation failed during writing to log in OvercommitTracker\n") != -1)  \
                ;                                                                                         \
        }                                                                                                 \
    } while (false)

OvercommitResult OvercommitTracker::needToStopQuery(MemoryTracker * tracker, Int64 amount)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    if (OvercommitTrackerBlockerInThread::isBlocked())
        return OvercommitResult::NONE;
    // NOTE: Do not change the order of locks
    //
    // global_mutex must be acquired before overcommit_m, because
    // method OvercommitTracker::onQueryStop(MemoryTracker *) is
    // always called with already acquired global_mutex in
    // ProcessListEntry::~ProcessListEntry().
    std::unique_lock<std::mutex> global_lock(global_mutex);
    std::unique_lock<std::mutex> lk(overcommit_m);

    size_t id = next_id++;

    auto max_wait_time = tracker->getOvercommitWaitingTime();

    if (max_wait_time == ZERO_MICROSEC)
        return OvercommitResult::DISABLED;

    pickQueryToExclude();
    assert(cancellation_state != QueryCancellationState::NONE);
    global_lock.unlock();

    // If no query was chosen we need to stop current query.
    // This may happen if no soft limit is set.
    if (picked_tracker == nullptr)
    {
        // Here state can not be RUNNING, because it requires
        // picked_tracker to be not null pointer.
        assert(cancellation_state == QueryCancellationState::SELECTED);
        cancellation_state = QueryCancellationState::NONE;
        return OvercommitResult::DISABLED;
    }
    if (picked_tracker == tracker)
    {
        // Query of the provided as an argument memory tracker was chosen.
        // It may happen even when current state is RUNNING, because
        // ThreadStatus::~ThreadStatus may call MemoryTracker::alloc.
        cancellation_state = QueryCancellationState::RUNNING;
        return OvercommitResult::SELECTED;
    }

    allow_release = true;

    required_memory += amount;
    auto wait_start_time = std::chrono::system_clock::now();
    bool timeout = !cv.wait_for(lk, max_wait_time, [this, id]()
    {
        return id < id_to_release || cancellation_state == QueryCancellationState::NONE;
    });
    auto wait_end_time = std::chrono::system_clock::now();
    ProfileEvents::increment(ProfileEvents::MemoryOvercommitWaitTimeMicroseconds, (wait_end_time - wait_start_time) / 1us);
    LOG_DEBUG_SAFE(getLogger(), "Memory was{} freed within timeout", (timeout ? " not" : ""));

    required_memory -= amount;
    bool still_need = !(id < id_to_release); // True if thread wasn't released

    // If threads where not released since last call of this method,
    // we can release them now.
    if (allow_release && required_memory <= freed_memory && still_need)
        releaseThreads();

    // All required amount of memory is free now and selected query to stop doesn't know about it.
    // As we don't need to free memory, we can continue execution of the selected query.
    if (required_memory == 0 && cancellation_state == QueryCancellationState::SELECTED)
        reset();
    if (timeout)
        return OvercommitResult::TIMEOUTED;
    if (still_need)
        return OvercommitResult::NOT_ENOUGH_FREED;
    else
        return OvercommitResult::MEMORY_FREED;
}

void OvercommitTracker::tryContinueQueryExecutionAfterFree(Int64 amount)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    if (OvercommitTrackerBlockerInThread::isBlocked())
        return;

    std::lock_guard guard(overcommit_m);
    if (cancellation_state != QueryCancellationState::NONE)
    {
        freed_memory += amount;
        if (freed_memory >= required_memory)
            releaseThreads();
    }
}

void OvercommitTracker::onQueryStop(MemoryTracker * tracker)
{
    DENY_ALLOCATIONS_IN_SCOPE;

    std::lock_guard lk(overcommit_m);
    if (picked_tracker == tracker)
    {
        LOG_DEBUG_SAFE(getLogger(), "Picked query stopped");

        reset();
        cv.notify_all();
    }
}

void OvercommitTracker::releaseThreads()
{
    id_to_release = next_id;
    freed_memory = 0;
    allow_release = false; // To avoid repeating call of this method in OvercommitTracker::needToStopQuery
    cv.notify_all();
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
    // This is guaranteed by locking global_mutex in OvercommitTracker::needToStopQuery.
    auto & queries = user_process_list->queries;
    LOG_DEBUG_SAFE(logger, "Trying to choose query to stop from {} queries", queries.size());
    for (auto const & query : queries)
    {
        if (query.second->isKilled())
            continue;

        auto * memory_tracker = query.second->getMemoryTracker();
        if (!memory_tracker)
            continue;

        auto ratio = memory_tracker->getOvercommitRatio();
        LOG_DEBUG_SAFE(logger, "Query has ratio {}/{}", ratio.committed, ratio.soft_limit);
        if (ratio.soft_limit != 0 && current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    }
    LOG_DEBUG_SAFE(logger, "Selected to stop query with overcommit ratio {}/{}",
        current_ratio.committed, current_ratio.soft_limit);
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
    // This is guaranteed by locking global_mutex in OvercommitTracker::needToStopQuery.
    LOG_DEBUG_SAFE(logger, "Trying to choose query to stop from {} queries", process_list->size());
    for (auto const & query : process_list->processes)
    {
        if (query.isKilled())
            continue;

        Int64 user_soft_limit = 0;
        if (auto const * user_process_list = query.getUserProcessList())
            user_soft_limit = user_process_list->user_memory_tracker.getSoftLimit();
        if (user_soft_limit == 0)
            continue;

        auto * memory_tracker = query.getMemoryTracker();
        if (!memory_tracker)
            continue;
        auto ratio = memory_tracker->getOvercommitRatio(user_soft_limit);
        LOG_DEBUG_SAFE(logger, "Query has ratio {}/{}", ratio.committed, ratio.soft_limit);
        if (current_ratio < ratio)
        {
            query_tracker = memory_tracker;
            current_ratio   = ratio;
        }
    }
    LOG_DEBUG_SAFE(logger, "Selected to stop query with overcommit ratio {}/{}",
        current_ratio.committed, current_ratio.soft_limit);
    picked_tracker = query_tracker;
}

thread_local size_t OvercommitTrackerBlockerInThread::counter = 0;
