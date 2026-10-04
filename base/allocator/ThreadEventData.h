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
enum TeAlloc : unsigned
{
    te_alloc_prof_sample,
    te_alloc_stats_interval,
    te_alloc_tcache_gc,
    te_alloc_peak,
    te_alloc_user0,
    te_alloc_user1,
    te_alloc_user2,
    te_alloc_user3,
    te_alloc_last = te_alloc_user3,
    te_alloc_count = te_alloc_last + 1,
};

/// jemalloc: te_dalloc_t
enum TeDalloc : unsigned
{
    te_dalloc_tcache_gc,
    te_dalloc_peak,
    te_dalloc_user0,
    te_dalloc_user1,
    te_dalloc_user2,
    te_dalloc_user3,
    te_dalloc_last = te_dalloc_user3,
    te_dalloc_count = te_dalloc_last + 1,
};

/// jemalloc: TE_MAX_USER_EVENTS
inline constexpr unsigned TE_MAX_USER_EVENTS = 4;

/// The remaining wait (in bytes) of every event.
/// jemalloc: te_data_t, TE_DATA_INITIALIZER
struct ThreadEventData
{
    uint64_t alloc_wait[te_alloc_count] = {};
    uint64_t dalloc_wait[te_dalloc_count] = {};
};

static_assert(sizeof(ThreadEventData) == 112, "Must have the size of te_data_t");

/// jemalloc: peak_t, PEAK_INITIALIZER
struct Peak
{
    /// The highest recorded peak value, after adjustment (see below).
    uint64_t cur_max = 0;
    /// The difference between alloc and dalloc at the last `setZero` call; this lets us cancel out the appropriate
    /// amount of excess.
    uint64_t adjustment = 0;

    /// jemalloc: peak_max
    uint64_t max() const { return cur_max; }

    /// jemalloc: peak_update
    void update(uint64_t alloc, uint64_t dalloc)
    {
        int64_t candidate_max = static_cast<int64_t>(alloc - dalloc - adjustment);
        if (candidate_max > static_cast<int64_t>(cur_max))
            cur_max = static_cast<uint64_t>(candidate_max);
    }

    /// Resets the counter to zero; all peaks are now relative to this point.
    /// jemalloc: peak_set_zero
    void setZero(uint64_t alloc, uint64_t dalloc)
    {
        cur_max = 0;
        adjustment = alloc - dalloc;
    }
};

/// jemalloc: activity_callback_t
using ActivityCallback = void (*)(void * uctx, uint64_t allocated, uint64_t deallocated);

/// The `experimental.thread.activity_callback` thunk, called by the peak event.
/// jemalloc: activity_callback_thunk_t, ACTIVITY_CALLBACK_THUNK_INITIALIZER
struct ActivityCallbackThunk
{
    ActivityCallback callback = nullptr;
    void * uctx = nullptr;
};

}
