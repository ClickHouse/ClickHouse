/// Per size class statistics of the sampled allocations (`prof.stats.*`, `opt.prof_stats`)
/// (jemalloc: `prof_stats.c`).

#include <allocator/Profiling.h>

#include <allocator/Options.h>

#include <cstring>

namespace jemalloc
{

constinit Mutex profiling_stats_mutex;

namespace
{

/// jemalloc: prof_stats_live, prof_stats_accum
constinit ProfilingStats profiling_stats_live[SIZE_CLASS_NUM_SIZES] = {};
constinit ProfilingStats profiling_stats_accumulated[SIZE_CLASS_NUM_SIZES] = {};

/// jemalloc: prof_stats_enter
void profilingStatsEnter(ThreadState & thread_state, SizeClassIdx idx)
{
    ALLOCATOR_ASSERT(options.profiling && options.profiling_stats);
    ALLOCATOR_ASSERT(idx < SIZE_CLASS_NUM_SIZES);
    (void)idx;
    profiling_stats_mutex.lock(&thread_state);
}

/// jemalloc: prof_stats_leave
void profilingStatsLeave(ThreadState & thread_state)
{
    profiling_stats_mutex.unlock(&thread_state);
}

}

/// jemalloc: prof_stats_inc
void profilingStatsIncrement(ThreadState & thread_state, SizeClassIdx idx, size_t size)
{
    profilingStatsEnter(thread_state, idx);
    profiling_stats_live[idx].request_sum += size;
    ++profiling_stats_live[idx].count;
    profiling_stats_accumulated[idx].request_sum += size;
    ++profiling_stats_accumulated[idx].count;
    profilingStatsLeave(thread_state);
}

/// jemalloc: prof_stats_dec
void profilingStatsDecrement(ThreadState & thread_state, SizeClassIdx idx, size_t size)
{
    profilingStatsEnter(thread_state, idx);
    profiling_stats_live[idx].request_sum -= size;
    --profiling_stats_live[idx].count;
    profilingStatsLeave(thread_state);
}

/// jemalloc: prof_stats_get_live
void profilingStatsGetLive(ThreadState & thread_state, SizeClassIdx idx, ProfilingStats * stats)
{
    profilingStatsEnter(thread_state, idx);
    memcpy(stats, &profiling_stats_live[idx], sizeof(ProfilingStats));
    profilingStatsLeave(thread_state);
}

/// jemalloc: prof_stats_get_accum
void profilingStatsGetAccumulated(ThreadState & thread_state, SizeClassIdx idx, ProfilingStats * stats)
{
    profilingStatsEnter(thread_state, idx);
    memcpy(stats, &profiling_stats_accumulated[idx], sizeof(ProfilingStats));
    profilingStatsLeave(thread_state);
}

}
