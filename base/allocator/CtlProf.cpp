/// `prof.*`, `experimental.hooks.prof_*`, `experimental.prof_recent.*` (jemalloc: `ctl.c`).

#include <allocator/CtlImpl.h>

#include <allocator/Options.h>
#include <allocator/Prof.h>
#include <allocator/ProfHooks.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

namespace jemalloc::ctl
{

/// jemalloc: prof_thread_active_init_ctl
int profThreadActiveInit(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::prof)
        return ENOENT;

    bool oldval;
    if (newp != nullptr)
    {
        if (!opt.prof)
            return ENOENT;
        if (newlen != sizeof(bool))
            return EINVAL;
        oldval = profThreadActiveInitSet(&tsd, *static_cast<bool *>(newp));
    }
    else
    {
        oldval = opt.prof ? profThreadActiveInitGet(&tsd) : false;
    }
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: prof_active_ctl
int profActive(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::prof)
        return ENOENT;

    bool oldval;
    if (newp != nullptr)
    {
        if (newlen != sizeof(bool))
            return EINVAL;
        bool val = *static_cast<bool *>(newp);
        if (!opt.prof)
        {
            if (val)
                return ENOENT;
            /// No change needed (already off).
            oldval = false;
        }
        else
        {
            oldval = profActiveSet(&tsd, val);
        }
    }
    else
    {
        oldval = opt.prof ? profActiveGet(&tsd) : false;
    }
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: prof_dump_ctl
int profDump(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof))
        return ENOENT;

    const char * filename = nullptr;
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    if (int ret = write(newp, newlen, filename))
        return ret;

    if (profMdump(tsd, filename))
        return EFAULT;
    return 0;
}

/// jemalloc: prof_gdump_ctl
int profGdump(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::prof)
        return ENOENT;

    bool oldval;
    if (newp != nullptr)
    {
        if (!opt.prof)
            return ENOENT;
        if (newlen != sizeof(bool))
            return EINVAL;
        oldval = profGdumpSet(&tsd, *static_cast<bool *>(newp));
    }
    else
    {
        oldval = opt.prof ? profGdumpGet(&tsd) : false;
    }
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: prof_prefix_ctl
int profPrefix(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof))
        return ENOENT;

    const char * prefix = nullptr;
    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    if (int ret = write(newp, newlen, prefix))
        return ret;

    return profPrefixSet(&tsd, prefix) ? EFAULT : 0;
}

/// jemalloc: prof_reset_ctl
int profReset(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    size_t lg_sample = lg_prof_sample;

    if (!(config::prof && opt.prof))
        return ENOENT;

    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    if (int ret = write(newp, newlen, lg_sample))
        return ret;
    if (lg_sample >= (sizeof(uint64_t) << 3))
        lg_sample = (sizeof(uint64_t) << 3) - 1;

    jemalloc::profReset(tsd, lg_sample);
    return 0;
}

/// jemalloc: prof_interval_ctl, lg_prof_sample_ctl (CTL_RO_NL_CGEN(config_prof, ...))
int profInterval(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyNlIf<uint64_t, [] { return config::prof; }, [] { return prof_interval; }>(
        tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

int profLgSample(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyNlIf<size_t, [] { return config::prof; }, [] { return lg_prof_sample; }>(
        tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// `prof_log` is dropped. jemalloc: prof_log_start_ctl, prof_log_stop_ctl
JE_CTL_DROPPED(profLogStart)
JE_CTL_DROPPED(profLogStop)

namespace
{

enum class ProfStatsKind
{
    BinsLive,
    BinsAccum,
    LextentsLive,
    LextentsAccum,
};

/// jemalloc: prof_stats_bins_i_live_ctl, prof_stats_bins_i_accum_ctl, prof_stats_lextents_i_live_ctl,
/// prof_stats_lextents_i_accum_ctl
template <ProfStatsKind kind>
int profStatsLeaf(ThreadState & tsd, const size_t * mib, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof && opt.prof_stats))
        return ENOENT;

    if (int ret = readOnly(newp, newlen))
        return ret;
    unsigned ind;
    if (int ret = mibUnsigned(mib, 3, ind))
        return ret;

    constexpr bool bins = kind == ProfStatsKind::BinsLive || kind == ProfStatsKind::BinsAccum;
    constexpr bool live = kind == ProfStatsKind::BinsLive || kind == ProfStatsKind::LextentsLive;
    if (ind >= (bins ? SC_NBINS : SC_NSIZES - SC_NBINS))
        return EINVAL;
    auto szind = static_cast<szind_t>(bins ? ind : ind + SC_NBINS);

    ProfStats stats;
    if constexpr (live)
        profStatsGetLive(tsd, szind, &stats);
    else
        profStatsGetAccum(tsd, szind, &stats);
    return read(oldp, oldlenp, stats);
}

}

int profStatsBinsILive(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profStatsLeaf<ProfStatsKind::BinsLive>(tsd, mib, oldp, oldlenp, newp, newlen);
}

int profStatsBinsIAccum(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profStatsLeaf<ProfStatsKind::BinsAccum>(tsd, mib, oldp, oldlenp, newp, newlen);
}

int profStatsLextentsILive(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profStatsLeaf<ProfStatsKind::LextentsLive>(tsd, mib, oldp, oldlenp, newp, newlen);
}

int profStatsLextentsIAccum(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profStatsLeaf<ProfStatsKind::LextentsAccum>(tsd, mib, oldp, oldlenp, newp, newlen);
}

/// jemalloc: prof_stats_bins_i_index
bool profStatsBinsIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    if (!(config::prof && opt.prof && opt.prof_stats))
        return false;
    return i < SC_NBINS;
}

/// jemalloc: prof_stats_lextents_i_index
bool profStatsLextentsIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    if (!(config::prof && opt.prof && opt.prof_stats))
        return false;
    return i < SC_NSIZES - SC_NBINS;
}

namespace
{

/// The common part of `experimental.hooks.prof_*`: both `oldp` and `newp` null -> `EINVAL`; reads the old hook if
/// `oldp`; if `newp`: `ENOENT` without `opt.prof`, `WRITE`, then (if `reject_null`) `EINVAL` for a null hook, and
/// stores it.
/// jemalloc: experimental_hooks_prof_backtrace_ctl, experimental_hooks_prof_dump_ctl,
/// experimental_hooks_prof_sample_ctl, experimental_hooks_prof_sample_free_ctl
template <typename Hook, Hook (*get)(), void (*set)(Hook), bool reject_null>
int profHookLeaf(void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (oldp == nullptr && newp == nullptr)
        return EINVAL;
    if (oldp != nullptr)
    {
        Hook old_hook = get();
        if (int ret = read(oldp, oldlenp, old_hook))
            return ret;
    }
    if (newp != nullptr)
    {
        if (!opt.prof)
            return ENOENT;
        Hook new_hook = nullptr;
        if (int ret = write(newp, newlen, new_hook))
            return ret;
        if (reject_null && new_hook == nullptr)
            return EINVAL;
        set(new_hook);
    }
    return 0;
}

}

int experimentalHooksProfBacktrace(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profHookLeaf<ProfBacktraceHook, profBacktraceHookGet, profBacktraceHookSet, true>(oldp, oldlenp, newp, newlen);
}

int experimentalHooksProfDump(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profHookLeaf<ProfDumpHook, profDumpHookGet, profDumpHookSet, false>(oldp, oldlenp, newp, newlen);
}

int experimentalHooksProfSample(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profHookLeaf<ProfSampleHook, profSampleHookGet, profSampleHookSet, false>(oldp, oldlenp, newp, newlen);
}

int experimentalHooksProfSampleFree(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return profHookLeaf<ProfSampleFreeHook, profSampleFreeHookGet, profSampleFreeHookSet, false>(oldp, oldlenp, newp, newlen);
}

/// jemalloc: experimental_prof_recent_alloc_max_ctl
int experimentalProfRecentAllocMax(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof))
        return ENOENT;

    ssize_t old_max;
    if (newp != nullptr)
    {
        if (newlen != sizeof(ssize_t))
            return EINVAL;
        ssize_t max = *static_cast<ssize_t *>(newp);
        if (max < -1)
            return EINVAL;
        old_max = profRecentAllocMaxCtlWrite(tsd, max);
    }
    else
    {
        old_max = profRecentAllocMaxCtlRead();
    }
    return read(oldp, oldlenp, old_max);
}

namespace
{

/// jemalloc: write_cb_packet_t
struct WriteCallbackPacket
{
    WriteCallback * write_cb;
    void * cbopaque;
};

static_assert(sizeof(WriteCallbackPacket) == sizeof(void *) * 2);

}

/// jemalloc: experimental_prof_recent_alloc_dump_ctl
int experimentalProfRecentAllocDump(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof))
        return ENOENT;

    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    WriteCallbackPacket write_cb_packet;
    if (int ret = assuredWrite(newp, newlen, write_cb_packet))
        return ret;

    profRecentAllocDump(tsd, write_cb_packet.write_cb, write_cb_packet.cbopaque);
    return 0;
}

}
