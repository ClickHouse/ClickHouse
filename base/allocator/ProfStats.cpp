/// Per size class statistics of the sampled allocations (`prof.stats.*`, `opt.prof_stats`)
/// (jemalloc: `prof_stats.c`).

#include <allocator/Prof.h>

#include <allocator/Options.h>

#include <cstring>

namespace jemalloc
{

constinit Mutex prof_stats_mtx;

namespace
{

/// jemalloc: prof_stats_live, prof_stats_accum
constinit ProfStats prof_stats_live[SC_NSIZES] = {};
constinit ProfStats prof_stats_accum[SC_NSIZES] = {};

/// jemalloc: prof_stats_enter
void profStatsEnter(ThreadState & tsd, szind_t ind)
{
    JE_ASSERT(opt.prof && opt.prof_stats);
    JE_ASSERT(ind < SC_NSIZES);
    (void)ind;
    prof_stats_mtx.lock(&tsd);
}

/// jemalloc: prof_stats_leave
void profStatsLeave(ThreadState & tsd)
{
    prof_stats_mtx.unlock(&tsd);
}

}

/// jemalloc: prof_stats_inc
void profStatsInc(ThreadState & tsd, szind_t ind, size_t size)
{
    profStatsEnter(tsd, ind);
    prof_stats_live[ind].req_sum += size;
    ++prof_stats_live[ind].count;
    prof_stats_accum[ind].req_sum += size;
    ++prof_stats_accum[ind].count;
    profStatsLeave(tsd);
}

/// jemalloc: prof_stats_dec
void profStatsDec(ThreadState & tsd, szind_t ind, size_t size)
{
    profStatsEnter(tsd, ind);
    prof_stats_live[ind].req_sum -= size;
    --prof_stats_live[ind].count;
    profStatsLeave(tsd);
}

/// jemalloc: prof_stats_get_live
void profStatsGetLive(ThreadState & tsd, szind_t ind, ProfStats * stats)
{
    profStatsEnter(tsd, ind);
    memcpy(stats, &prof_stats_live[ind], sizeof(ProfStats));
    profStatsLeave(tsd);
}

/// jemalloc: prof_stats_get_accum
void profStatsGetAccum(ThreadState & tsd, szind_t ind, ProfStats * stats)
{
    profStatsEnter(tsd, ind);
    memcpy(stats, &prof_stats_accum[ind], sizeof(ProfStats));
    profStatsLeave(tsd);
}

}
