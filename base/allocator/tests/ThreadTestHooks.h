#pragma once

/// Test definitions of the hooks that `ThreadState` and `ThreadEvent` call into other modules (tcache, arenas, prof,
/// stats). They record every call in a log. Include this header in exactly one translation unit of a test.

#include <allocator/Prng.h>
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
    jemalloc::ThreadState * tsd;
    uint8_t state;
    int8_t reentrancy_level;
    uint64_t allocated;
    uint64_t deallocated;
};

inline std::mutex hook_mutex;
inline std::vector<HookCall> hook_log;

inline void record(const char * name, jemalloc::ThreadState & tsd)
{
    std::lock_guard lock(hook_mutex);
    hook_log.push_back({name, &tsd, tsd.stateGet(), tsd.reentrancy_level, tsd.thread_allocated, tsd.thread_deallocated});
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

/// The global `lg_prof_sample` of the prof module.
inline unsigned lg_prof_sample = 19;

/// jemalloc's `prof_sample_new_event_wait` (`prof.c`), applied to an explicit PRNG state.
inline uint64_t profGeometricWait(uint64_t & prng_state, unsigned lg_sample)
{
    if (lg_sample == 0)
        return jemalloc::TE_MIN_START_WAIT;
    uint64_t r = jemalloc::prngLgRangeU64(prng_state, 53);
    double u = (r == 0U) ? 1.0 : double(static_cast<long double>(r) * (1.0L / 9007199254740992.0L));
    return uint64_t(std::log(u) / std::log(1.0 - (1.0 / double(uint64_t(1) << lg_sample)))) + uint64_t(1);
}

}

namespace jemalloc
{

bool tcacheTsdDataInit(ThreadState & tsd)
{
    thread_test::record("tcacheTsdDataInit", tsd);
    tsd.tcache_enabled = opt.tcache;
    tsd.slowUpdate();
    return false;
}

void tcacheCleanup(ThreadState & tsd)
{
    thread_test::record("tcacheCleanup", tsd);
}

void arenaCleanup(ThreadState & tsd)
{
    thread_test::record("arenaCleanup", tsd);
}

void iarenaCleanup(ThreadState & tsd)
{
    thread_test::record("iarenaCleanup", tsd);
}

void profTdataCleanup(ThreadState & tsd)
{
    thread_test::record("profTdataCleanup", tsd);
}

void * a0malloc(size_t size)
{
    return std::aligned_alloc(CACHELINE, alignmentCeiling(size, CACHELINE));
}

void a0dalloc(void * ptr)
{
    std::free(ptr);
}

uint64_t tcacheGcNewEventWait(ThreadState &)
{
    return opt.tcache_gc_incr_bytes;
}

uint64_t tcacheGcPostponedEventWait(ThreadState &)
{
    return TE_MIN_START_WAIT;
}

void tcacheGcEvent(ThreadState & tsd)
{
    thread_test::record("tcacheGcEvent", tsd);
}

uint64_t profSampleNewEventWait(ThreadState & tsd)
{
    return thread_test::profGeometricWait(tsd.prng_state, thread_test::lg_prof_sample);
}

uint64_t profSamplePostponedEventWait(ThreadState & tsd)
{
    return profSampleNewEventWait(tsd);
}

void profSampleEvent(ThreadState & tsd)
{
    thread_test::record("profSampleEvent", tsd);
}

constinit uint64_t stats_interval_accum_batch = 0;

uint64_t statsIntervalNewEventWait(ThreadState &)
{
    return stats_interval_accum_batch;
}

uint64_t statsIntervalPostponedEventWait(ThreadState &)
{
    return TE_MIN_START_WAIT;
}

void statsIntervalEvent(ThreadState & tsd)
{
    thread_test::record("statsIntervalEvent", tsd);
}

}
