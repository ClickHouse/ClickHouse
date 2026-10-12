/// `prof.*`, `experimental.hooks.prof_*`, `experimental.prof_recent.*` (jemalloc: `ctl.c`).

#include <allocator/MallctlImpl.h>

#include <allocator/Options.h>
#include <allocator/Profiling.h>
#include <allocator/ProfilingHooks.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

namespace jemalloc::mallctl
{

/// jemalloc: prof_thread_active_init_ctl
int profilingThreadActiveInit(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::profiling)
        return ENOENT;

    bool old_setting;
    if (new_value != nullptr)
    {
        if (!options.profiling)
            return ENOENT;
        if (new_length != sizeof(bool))
            return EINVAL;
        old_setting = profilingThreadActiveInitSet(&thread_state, *static_cast<bool *>(new_value));
    }
    else
    {
        old_setting = options.profiling ? profilingThreadActiveInitGet(&thread_state) : false;
    }
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: prof_active_ctl
int profilingActive(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::profiling)
        return ENOENT;

    bool old_setting;
    if (new_value != nullptr)
    {
        if (new_length != sizeof(bool))
            return EINVAL;
        bool value = *static_cast<bool *>(new_value);
        if (!options.profiling)
        {
            if (value)
                return ENOENT;
            /// No change needed (already off).
            old_setting = false;
        }
        else
        {
            old_setting = profilingActiveSet(&thread_state, value);
        }
    }
    else
    {
        old_setting = options.profiling ? profilingActiveGet(&thread_state) : false;
    }
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: prof_dump_ctl
int profilingDump(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling))
        return ENOENT;

    const char * filename = nullptr;
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    if (int result = write(new_value, new_length, filename))
        return result;

    if (profilingManualDump(thread_state, filename))
        return EFAULT;
    return 0;
}

/// jemalloc: prof_gdump_ctl
int profilingGrowthDump(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::profiling)
        return ENOENT;

    bool old_setting;
    if (new_value != nullptr)
    {
        if (!options.profiling)
            return ENOENT;
        if (new_length != sizeof(bool))
            return EINVAL;
        old_setting = profilingGrowthDumpSet(&thread_state, *static_cast<bool *>(new_value));
    }
    else
    {
        old_setting = options.profiling ? profilingGrowthDumpGet(&thread_state) : false;
    }
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: prof_prefix_ctl
int profilingPrefix(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling))
        return ENOENT;

    const char * prefix = nullptr;
    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    if (int result = write(new_value, new_length, prefix))
        return result;

    return profilingPrefixSet(&thread_state, prefix) ? EFAULT : 0;
}

/// jemalloc: prof_reset_ctl
int profilingReset(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    size_t log2_sample = log2_profiling_sample;

    if (!(config::profiling && options.profiling))
        return ENOENT;

    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    if (int result = write(new_value, new_length, log2_sample))
        return result;
    if (log2_sample >= (sizeof(uint64_t) << 3))
        log2_sample = (sizeof(uint64_t) << 3) - 1;

    jemalloc::profilingReset(thread_state, log2_sample);
    return 0;
}

/// jemalloc: prof_interval_ctl, lg_prof_sample_ctl (CTL_RO_NL_CGEN(config_prof, ...))
int profilingInterval(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyNoLockIf<uint64_t, [] { return config::profiling; }, [] { return profiling_interval; }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

int profilingLog2Sample(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyNoLockIf<size_t, [] { return config::profiling; }, [] { return log2_profiling_sample; }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// `profiling_log` is dropped. jemalloc: prof_log_start_ctl, prof_log_stop_ctl
ALLOCATOR_MALLCTL_DROPPED(profilingLogStart)
ALLOCATOR_MALLCTL_DROPPED(profilingLogStop)

namespace
{

enum class ProfilingStatsKind
{
    BinsLive,
    BinsAccumulated,
    LargeExtentsLive,
    LargeExtentsAccumulated,
};

/// jemalloc: prof_stats_bins_i_live_ctl, prof_stats_bins_i_accum_ctl, prof_stats_lextents_i_live_ctl,
/// prof_stats_lextents_i_accum_ctl
template <ProfilingStatsKind kind>
int profilingStatsLeaf(
    ThreadState & thread_state, const size_t * numeric_path, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling && options.profiling_stats))
        return ENOENT;

    if (int result = readOnly(new_value, new_length))
        return result;
    unsigned idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 3, idx))
        return result;

    constexpr bool bins = kind == ProfilingStatsKind::BinsLive || kind == ProfilingStatsKind::BinsAccumulated;
    constexpr bool live = kind == ProfilingStatsKind::BinsLive || kind == ProfilingStatsKind::LargeExtentsLive;
    if (idx >= (bins ? SIZE_CLASS_NUM_BINS : SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS))
        return EINVAL;
    auto size_class_idx = static_cast<SizeClassIdx>(bins ? idx : idx + SIZE_CLASS_NUM_BINS);

    ProfilingStats stats;
    if constexpr (live)
        profilingStatsGetLive(thread_state, size_class_idx, &stats);
    else
        profilingStatsGetAccumulated(thread_state, size_class_idx, &stats);
    return read(old_value, old_length_ptr, stats);
}

}

int profilingStatsBinsILive(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingStatsLeaf<ProfilingStatsKind::BinsLive>(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length);
}

int profilingStatsBinsIAccumulated(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingStatsLeaf<ProfilingStatsKind::BinsAccumulated>(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length);
}

int profilingStatsLargeExtentsILive(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingStatsLeaf<ProfilingStatsKind::LargeExtentsLive>(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length);
}

int profilingStatsLargeExtentsIAccumulated(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingStatsLeaf<ProfilingStatsKind::LargeExtentsAccumulated>(
        thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: prof_stats_bins_i_index
bool profilingStatsBinsIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    if (!(config::profiling && options.profiling && options.profiling_stats))
        return false;
    return i < SIZE_CLASS_NUM_BINS;
}

/// jemalloc: prof_stats_lextents_i_index
bool profilingStatsLargeExtentsIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    if (!(config::profiling && options.profiling && options.profiling_stats))
        return false;
    return i < SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS;
}

namespace
{

/// The common part of `experimental.hooks.prof_*`: both `old_value` and `new_value` null -> `EINVAL`; reads the old hook if
/// `old_value`; if `new_value`: `ENOENT` without `opt.prof`, `WRITE`, then (if `reject_null`) `EINVAL` for a null hook, and
/// stores it.
/// jemalloc: experimental_hooks_prof_backtrace_ctl, experimental_hooks_prof_dump_ctl,
/// experimental_hooks_prof_sample_ctl, experimental_hooks_prof_sample_free_ctl
template <typename Hook, Hook (*get)(), void (*set)(Hook), bool reject_null>
int profilingHookLeaf(void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (old_value == nullptr && new_value == nullptr)
        return EINVAL;
    if (old_value != nullptr)
    {
        Hook old_hook = get();
        if (int result = read(old_value, old_length_ptr, old_hook))
            return result;
    }
    if (new_value != nullptr)
    {
        if (!options.profiling)
            return ENOENT;
        Hook new_hook = nullptr;
        if (int result = write(new_value, new_length, new_hook))
            return result;
        if (reject_null && new_hook == nullptr)
            return EINVAL;
        set(new_hook);
    }
    return 0;
}

}

int experimentalHooksProfilingBacktrace(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingHookLeaf<ProfilingBacktraceHook, profilingBacktraceHookGet, profilingBacktraceHookSet, true>(
        old_value, old_length_ptr, new_value, new_length);
}

int experimentalHooksProfilingDump(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingHookLeaf<ProfilingDumpHook, profilingDumpHookGet, profilingDumpHookSet, false>(
        old_value, old_length_ptr, new_value, new_length);
}

int experimentalHooksProfilingSample(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingHookLeaf<ProfilingSampleHook, profilingSampleHookGet, profilingSampleHookSet, false>(
        old_value, old_length_ptr, new_value, new_length);
}

int experimentalHooksProfilingSampleFree(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return profilingHookLeaf<ProfilingSampleFreeHook, profilingSampleFreeHookGet, profilingSampleFreeHookSet, false>(
        old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: experimental_prof_recent_alloc_max_ctl
int experimentalProfilingRecentAllocMax(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling))
        return ENOENT;

    ssize_t old_max;
    if (new_value != nullptr)
    {
        if (new_length != sizeof(ssize_t))
            return EINVAL;
        ssize_t max = *static_cast<ssize_t *>(new_value);
        if (max < -1)
            return EINVAL;
        old_max = profilingRecentAllocMaxMallctlWrite(thread_state, max);
    }
    else
    {
        old_max = profilingRecentAllocMaxMallctlRead();
    }
    return read(old_value, old_length_ptr, old_max);
}

namespace
{

/// jemalloc: write_cb_packet_t
struct WriteCallbackPacket
{
    WriteCallback * write_callback;
    void * callback_argument;
};

static_assert(sizeof(WriteCallbackPacket) == sizeof(void *) * 2);

}

/// jemalloc: experimental_prof_recent_alloc_dump_ctl
int experimentalProfilingRecentAllocDump(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling))
        return ENOENT;

    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    WriteCallbackPacket write_callback_packet;
    if (int result = assuredWrite(new_value, new_length, write_callback_packet))
        return result;

    profilingRecentAllocDump(thread_state, write_callback_packet.write_callback, write_callback_packet.callback_argument);
    return 0;
}

}
