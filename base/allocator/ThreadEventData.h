#pragma once

/// The per-thread data of the thread events and the peak tracker, which live in `ThreadState`
/// (jemalloc: `thread_event_registry.h` (types), `peak.h`, `activity_callback.h`).
/// The event logic is in ThreadEvent.h.

#include <allocator/Common.h>

#include <cstdint>

namespace jemalloc
{

/// The allocation events ("te" is short for "thread_event"), in the order of jemalloc's `te_alloc_handlers` table
/// under ClickHouse's configuration (`JEMALLOC_PROF` and `JEMALLOC_STATS` are defined). The user event slots
/// (`experimental.hooks.thread_event`) are dropped, but their wait slots are kept so that `te_data_t` has the same
/// layout; they are never enabled.
/// jemalloc: te_alloc_t
enum ThreadEventAllocation : unsigned
{
    thread_event_allocation_profiling_sample,
    thread_event_allocation_stats_interval,
    thread_event_allocation_thread_cache_gc,
    thread_event_allocation_peak,
    thread_event_allocation_user0,
    thread_event_allocation_user1,
    thread_event_allocation_user2,
    thread_event_allocation_user3,
    thread_event_allocation_last = thread_event_allocation_user3,
    thread_event_allocation_count = thread_event_allocation_last + 1,
};

/// jemalloc: te_dalloc_t
enum ThreadEventDeallocation : unsigned
{
    thread_event_deallocation_thread_cache_gc,
    thread_event_deallocation_peak,
    thread_event_deallocation_user0,
    thread_event_deallocation_user1,
    thread_event_deallocation_user2,
    thread_event_deallocation_user3,
    thread_event_deallocation_last = thread_event_deallocation_user3,
    thread_event_deallocation_count = thread_event_deallocation_last + 1,
};

/// jemalloc: TE_MAX_USER_EVENTS
inline constexpr unsigned THREAD_EVENT_MAX_USER_EVENTS = 4;

/// The remaining wait (in bytes) of every event.
/// jemalloc: te_data_t, TE_DATA_INITIALIZER
struct ThreadEventData
{
    uint64_t alloc_wait[thread_event_allocation_count] = {};
    uint64_t deallocation_wait[thread_event_deallocation_count] = {};
};

static_assert(sizeof(ThreadEventData) == 112, "Must have the size of te_data_t");

/// jemalloc: peak_t, PEAK_INITIALIZER
struct Peak
{
    /// The highest recorded peak value, after adjustment (see below).
    uint64_t current_max = 0;
    /// The difference between alloc and dalloc at the last `setZero` call; this lets us cancel out the appropriate
    /// amount of excess.
    uint64_t adjustment = 0;

    /// jemalloc: peak_max
    uint64_t max() const { return current_max; }

    /// jemalloc: peak_update
    void update(uint64_t alloc, uint64_t deallocate)
    {
        int64_t candidate_max = static_cast<int64_t>(alloc - deallocate - adjustment);
        if (candidate_max > static_cast<int64_t>(current_max))
            current_max = static_cast<uint64_t>(candidate_max);
    }

    /// Resets the counter to zero; all peaks are now relative to this point.
    /// jemalloc: peak_set_zero
    void setZero(uint64_t alloc, uint64_t deallocate)
    {
        current_max = 0;
        adjustment = alloc - deallocate;
    }
};

/// jemalloc: activity_callback_t
using ActivityCallback = void (*)(void * user_context, uint64_t allocated, uint64_t deallocated);

/// The `experimental.thread.activity_callback` thunk, called by the peak event.
/// jemalloc: activity_callback_thunk_t, ACTIVITY_CALLBACK_THUNK_INITIALIZER
struct ActivityCallbackThunk
{
    ActivityCallback callback = nullptr;
    void * user_context = nullptr;
};

}
