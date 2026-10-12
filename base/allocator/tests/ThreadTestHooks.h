#pragma once

/// Test definitions of the hooks that `ThreadState` and `ThreadEvent` call into other modules (tcache, arenas, prof,
/// stats). They record every call in a log. Include this header in exactly one translation unit of a test.

#include <allocator/PRNG.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

namespace thread_test
{

struct HookCall
{
    std::string name;
    jemalloc::ThreadState * thread_state;
    uint8_t state;
    int8_t reentrancy_level;
    uint64_t allocated;
    uint64_t deallocated;
};

inline std::mutex hook_mutex;
inline std::vector<HookCall> hook_log;

inline void record(const char * name, jemalloc::ThreadState & thread_state)
{
    std::lock_guard lock(hook_mutex);
    hook_log.push_back(
        {name,
         &thread_state,
         thread_state.stateGet(),
         thread_state.reentrancy_level,
         thread_state.thread_allocated,
         thread_state.thread_deallocated});
}

inline std::vector<HookCall> takeLog()
{
    std::lock_guard lock(hook_mutex);
    std::vector<HookCall> result;
    result.swap(hook_log);
    return result;
}

/// The tests simulate an initialized allocator whose options do not force the slow paths (`malloc_slow` is true until
/// the initialization computes it).
inline const bool malloc_slow_reset = (jemalloc::malloc_slow = false, true);

/// The global `log2_profiling_sample` of the prof module.
inline unsigned log2_profiling_sample = 19;

/// jemalloc's `prof_sample_new_event_wait` (`prof.c`), applied to an explicit PRNG state.
inline uint64_t profilingGeometricWait(uint64_t & prng_state, unsigned log2_sample)
{
    if (log2_sample == 0)
        return jemalloc::THREAD_EVENT_MIN_START_WAIT;
    uint64_t r = jemalloc::prngLog2RangeU64(prng_state, 53);
    double u = (r == 0U) ? 1.0 : double(static_cast<long double>(r) * (1.0L / 9007199254740992.0L));
    return uint64_t(std::log(u) / std::log(1.0 - (1.0 / double(uint64_t(1) << log2_sample)))) + uint64_t(1);
}

}

namespace jemalloc
{

bool threadCacheThreadStateDataInit(ThreadState & thread_state)
{
    thread_test::record("tcacheTsdDataInit", thread_state);
    thread_state.thread_cache_enabled = options.thread_cache;
    thread_state.slowUpdate();
    return false;
}

void threadCacheCleanup(ThreadState & thread_state)
{
    thread_test::record("tcacheCleanup", thread_state);
}

void arenaCleanup(ThreadState & thread_state)
{
    thread_test::record("arenaCleanup", thread_state);
}

void internalArenaCleanup(ThreadState & thread_state)
{
    thread_test::record("iarenaCleanup", thread_state);
}

void profilingThreadDataCleanup(ThreadState & thread_state)
{
    thread_test::record("profTdataCleanup", thread_state);
}

void * arena0Allocate(size_t size)
{
    return std::aligned_alloc(CACHE_LINE, alignmentCeiling(size, CACHE_LINE));
}

void arena0Deallocate(void * ptr)
{
    std::free(ptr);
}

uint64_t threadCacheGCNewEventWait(ThreadState &)
{
    return options.thread_cache_gc_increment_bytes;
}

uint64_t threadCacheGCPostponedEventWait(ThreadState &)
{
    return THREAD_EVENT_MIN_START_WAIT;
}

void threadCacheGCEvent(ThreadState & thread_state)
{
    thread_test::record("tcacheGcEvent", thread_state);
}

uint64_t profilingSampleNewEventWait(ThreadState & thread_state)
{
    return thread_test::profilingGeometricWait(thread_state.prng_state, thread_test::log2_profiling_sample);
}

uint64_t profilingSamplePostponedEventWait(ThreadState & thread_state)
{
    return profilingSampleNewEventWait(thread_state);
}

void profilingSampleEvent(ThreadState & thread_state)
{
    thread_test::record("profSampleEvent", thread_state);
}

constinit uint64_t stats_interval_accumulated_batch = 0;

uint64_t statsIntervalNewEventWait(ThreadState &)
{
    return stats_interval_accumulated_batch;
}

uint64_t statsIntervalPostponedEventWait(ThreadState &)
{
    return THREAD_EVENT_MIN_START_WAIT;
}

void statsIntervalEvent(ThreadState & thread_state)
{
    thread_test::record("statsIntervalEvent", thread_state);
}

}
