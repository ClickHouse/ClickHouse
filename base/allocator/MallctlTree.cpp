/// The `mallctl` tree (jemalloc: `ctl.c`, the `*_node` arrays).
///
/// The children of every node are listed in exactly jemalloc's order: numeric path components are positions in these arrays.
/// The leaves whose value is a constant, an option or a configuration flag (`config.*`, `opt.*`, most of `arenas.*`),
/// the leaves of the dropped HPA/SEC statistics (always zero) and the mutex profiling leaves are generated here from
/// the templates of MallctlImpl.h; all other leaves are declared in MallctlImpl.h and defined in the file of their subtree.

#include <allocator/Mallctl.h>

#include <allocator/ExtentHooks.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Nanoseconds.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include <cstddef>

/// Defined in MallocConf.cpp. jemalloc: je_malloc_conf, je_malloc_conf_2_conf_harder
extern "C" const char * je_malloc_conf;
extern "C" const char * je_malloc_conf_2_conf_harder;

namespace jemalloc
{

namespace
{

using mallctl::MutexProfilingCounter;

/// --- Node constructors ---------------------------------------------------------------------------------------------

/// jemalloc: {NAME(n), CTL(c)}
constexpr MallctlNode leaf(const char * name, MallctlLeafFunction function)
{
    return {name, nullptr, 0, nullptr, function};
}

/// jemalloc: {NAME(n), CHILD(named, c)}
template <size_t N>
constexpr MallctlNode named(const char * name, const MallctlNode (&children)[N])
{
    return {name, children, N, nullptr, nullptr};
}

/// jemalloc: {NAME(n), CHILD(indexed, c)} with `c_node[] = {{INDEX(i)}}`; `super` is the node returned by the
/// index function (`super_i_node`).
constexpr MallctlNode indexed(const char * name, MallctlIndexFunction index, const MallctlNode (&super)[1])
{
    return {name, super, 1, index, nullptr};
}

/// jemalloc: super_*_node[] = {{NAME(""), CHILD(named, *)}}
template <size_t N>
constexpr MallctlNode super(const MallctlNode (&children)[N])
{
    return {"", children, N, nullptr, nullptr};
}

/// jemalloc: CTL_RO_NL_GEN, CTL_RO_CONFIG_GEN
template <typename T, auto get>
constexpr MallctlNode readOnlyLeaf(const char * name)
{
    return leaf(name, &mallctl::readOnlyNoLock<T, get>);
}

/// jemalloc: CTL_RO_NL_CGEN
template <typename T, auto condition, auto get>
constexpr MallctlNode readOnlyLeafIf(const char * name)
{
    return leaf(name, &mallctl::readOnlyNoLockIf<T, condition, get>);
}

constexpr bool statsEnabled()
{
    return config::stats;
}

/// jemalloc: CTL_RO_CGEN(config_stats, ...)
template <typename T, auto get>
constexpr MallctlNode readOnlyStatsLeaf(const char * name)
{
    return leaf(name, &mallctl::readOnlyLockedIf<T, statsEnabled, get>);
}

/// A statistic of a dropped feature (HPA, SEC): `CTL_RO_CGEN(config_stats, ...)` of a value that is always zero.
template <typename T>
constexpr MallctlNode zeroStat(const char * name)
{
    return readOnlyStatsLeaf<T, [] { return T(0); }>(name);
}

/// jemalloc: MUTEX_PROF_DATA_NODE
template <auto accessor>
constexpr MallctlNode mutex_profiling_node[] = {
    leaf("num_ops", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::NumOps>),
    leaf("num_wait", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::NumWait>),
    leaf("num_spin_acq", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::NumSpinAcquired>),
    leaf("num_owner_switch", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::NumOwnerSwitch>),
    leaf("total_wait_time", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::TotalWaitTime>),
    leaf("max_wait_time", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::MaxWaitTime>),
    /// Note that # of current waiting thread not provided.
    leaf("max_num_thds", &mallctl::mutexProfiling<accessor, MutexProfilingCounter::MaxNumThreads>),
};

/// --- Values of the generated leaves --------------------------------------------------------------------------------

/// jemalloc: `bin_infos[numeric_path[2]]`. The index function accepts `SC_NBINS` (off by one), where jemalloc reads past the
/// array; that bin reads as zeros here.
/// jemalloc compatibility: arenas_bin_i_index accepts i == SIZE_CLASS_NUM_BINS.
const BinInfo & binInfoOfNumericPath(const size_t * numeric_path)
{
    static constexpr BinInfo past_the_end{};
    return numeric_path[2] < SIZE_CLASS_NUM_BINS ? bin_infos[numeric_path[2]] : past_the_end;
}

/// jemalloc: `sz_index2size_unsafe(SC_NBINS + numeric_path[2])`. The index function accepts `SC_NSIZES - SC_NBINS` (off by
/// one), where jemalloc reads past `sz_index2size_tab`; that size reads as 0 here.
/// jemalloc compatibility: arenas_lextent_i_index accepts i == SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS.
size_t largeExtentSizeOfNumericPath(const size_t * numeric_path)
{
    return numeric_path[2] < SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS
        ? size_classes::indexToSizeUnsafe(SIZE_CLASS_NUM_BINS + static_cast<SizeClassIdx>(numeric_path[2]))
        : 0;
}

}

/// --- Constant index functions ----------------------------------------------------------------------------------------

namespace mallctl
{

/// jemalloc compatibility: `i > SC_NBINS` (not `>=`) is rejected, as in jemalloc.
/// jemalloc: arenas_bin_i_index
bool arenasBinIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    return !(i > SIZE_CLASS_NUM_BINS);
}

/// jemalloc compatibility: `i > SC_NSIZES - SC_NBINS` (not `>=`) is rejected, as in jemalloc.
/// jemalloc: arenas_lextent_i_index
bool arenasLargeExtentIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    return !(i > SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS);
}

/// jemalloc compatibility: `j > SC_NBINS` (not `>=`) is rejected, as in jemalloc (where `bins.<SC_NBINS>` reads
/// the beginning of `large_stats`).
/// jemalloc: stats_arenas_i_bins_j_index
bool statsArenasIBinsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return !(j > SIZE_CLASS_NUM_BINS);
}

/// jemalloc compatibility: `j > SC_NSIZES - SC_NBINS` (not `>=`) is rejected, as in jemalloc (where
/// `lextents.<SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS>` reads the beginning of `extent_stats`).
/// jemalloc: stats_arenas_i_lextents_j_index
bool statsArenasILargeExtentsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return !(j > SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS);
}

/// jemalloc: stats_arenas_i_extents_j_index
bool statsArenasIExtentsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return j < SIZE_CLASS_NUM_PAGE_SIZES;
}

/// jemalloc: PSSET_NPSIZES
inline constexpr size_t PAGE_SLAB_SET_NUM_PAGE_SIZES = 64;

/// jemalloc: stats_arenas_i_hpa_shard_nonfull_slabs_j_index
bool statsArenasIHugePageShardNonFullSlabsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return j < PAGE_SLAB_SET_NUM_PAGE_SIZES;
}

}

namespace
{

/// --- thread ----------------------------------------------------------------------------------------------------------

constexpr MallctlNode thread_cache_num_cached_max_node[] = {
    leaf("read_sizeclass", mallctl::threadCacheNumCachedMaxReadSizeClass),
    leaf("write", mallctl::threadCacheNumCachedMaxWrite),
};

constexpr MallctlNode thread_thread_cache_node[] = {
    leaf("enabled", mallctl::threadCacheEnabled),
    leaf("max", mallctl::threadCacheMax),
    leaf("flush", mallctl::threadThreadCacheFlush),
    named("ncached_max", thread_cache_num_cached_max_node),
};

constexpr MallctlNode thread_peak_node[] = {
    leaf("read", mallctl::threadPeakRead),
    leaf("reset", mallctl::threadPeakReset),
};

constexpr MallctlNode thread_profiling_node[] = {
    leaf("name", mallctl::threadProfilingName),
    leaf("active", mallctl::threadProfilingActive),
};

constexpr MallctlNode thread_node[] = {
    leaf("arena", mallctl::threadArena),
    leaf("allocated", mallctl::threadAllocated),
    leaf("allocatedp", mallctl::threadAllocatedPtr),
    leaf("deallocated", mallctl::threadDeallocated),
    leaf("deallocatedp", mallctl::threadDeallocatedPtr),
    named("tcache", thread_thread_cache_node),
    named("peak", thread_peak_node),
    named("prof", thread_profiling_node),
    leaf("idle", mallctl::threadIdle),
};

/// --- config (jemalloc: CTL_RO_CONFIG_GEN) ----------------------------------------------------------------------------

constexpr MallctlNode config_node[] = {
    readOnlyLeaf<bool, [] { return config::cache_oblivious; }>("cache_oblivious"),
    /// jemalloc's `config_debug` (`JEMALLOC_DEBUG`) is never enabled in ClickHouse; `ALLOCATOR_DEBUG` only enables
    /// our internal assertions.
    readOnlyLeaf<bool, [] { return false; }>("debug"),
    readOnlyLeaf<bool, [] { return config::fill; }>("fill"),
    readOnlyLeaf<bool, [] { return config::lazy_lock; }>("lazy_lock"),
    readOnlyLeaf<const char *, [] { return config::malloc_conf_default; }>("malloc_conf"),
    readOnlyLeaf<bool, [] { return config::option_safety_checks; }>("opt_safety_checks"),
    readOnlyLeaf<bool, [] { return config::profiling; }>("prof"),
    /// jemalloc: config_prof_libgcc (`JEMALLOC_PROF_LIBGCC`), config_prof_libunwind (`JEMALLOC_PROF_LIBUNWIND`),
    /// config_prof_frameptr (`JEMALLOC_PROF_FRAME_POINTER`): ClickHouse always uses libunwind.
    readOnlyLeaf<bool, [] { return false; }>("prof_libgcc"),
    readOnlyLeaf<bool, [] { return true; }>("prof_libunwind"),
    readOnlyLeaf<bool, [] { return false; }>("prof_frameptr"),
    readOnlyLeaf<bool, [] { return config::stats; }>("stats"),
    /// jemalloc: config_utrace (`JEMALLOC_UTRACE`), config_xmalloc (`JEMALLOC_XMALLOC`): never defined.
    readOnlyLeaf<bool, [] { return false; }>("utrace"),
    readOnlyLeaf<bool, [] { return false; }>("xmalloc"),
};

/// --- opt (jemalloc: CTL_RO_NL_GEN, CTL_RO_NL_CGEN) -------------------------------------------------------------------

constexpr bool configFill()
{
    return config::fill;
}

constexpr bool configProfiling()
{
    return config::profiling;
}

constexpr MallctlNode option_malloc_conf_node[] = {
    readOnlyLeafIf<const char *, [] { return options.malloc_conf_symlink != nullptr; }, [] { return options.malloc_conf_symlink; }>(
        "symlink"),
    readOnlyLeafIf<
        const char *,
        [] { return options.malloc_conf_env_variable != nullptr; },
        [] { return options.malloc_conf_env_variable; }>("env_var"),
    readOnlyLeafIf<const char *, [] { return je_malloc_conf != nullptr; }, [] { return je_malloc_conf; }>("global_var"),
    readOnlyLeafIf<const char *, [] { return je_malloc_conf_2_conf_harder != nullptr; }, [] { return je_malloc_conf_2_conf_harder; }>(
        "global_var_2_conf_harder"),
};

constexpr MallctlNode option_node[] = {
    readOnlyLeaf<bool, [] { return options.abort; }>("abort"),
    readOnlyLeaf<bool, [] { return options.abort_configuration; }>("abort_conf"),
    readOnlyLeaf<bool, [] { return options.cache_oblivious; }>("cache_oblivious"),
    readOnlyLeaf<bool, [] { return options.trust_madvise; }>("trust_madvise"),
    readOnlyLeaf<bool, [] { return options.experimental_huge_page_allocator_start_huge; }>("experimental_hpa_start_huge_if_thp_always"),
    readOnlyLeaf<bool, [] { return options.experimental_huge_page_allocator_enforce_hugify; }>("experimental_hpa_enforce_hugify"),
    readOnlyLeaf<bool, [] { return options.confirm_configuration; }>("confirm_conf"),
    readOnlyLeaf<bool, [] { return options.huge_page_allocator; }>("hpa"),
    readOnlyLeaf<size_t, [] { return options.huge_page_allocator_options.slab_max_alloc; }>("hpa_slab_max_alloc"),
    readOnlyLeaf<size_t, [] { return options.huge_page_allocator_options.hugification_threshold; }>("hpa_hugification_threshold"),
    readOnlyLeaf<uint64_t, [] { return options.huge_page_allocator_options.hugify_delay_ms; }>("hpa_hugify_delay_ms"),
    readOnlyLeaf<bool, [] { return options.huge_page_allocator_options.hugify_sync; }>("hpa_hugify_sync"),
    readOnlyLeaf<uint64_t, [] { return options.huge_page_allocator_options.min_purge_interval_ms; }>("hpa_min_purge_interval_ms"),
    readOnlyLeaf<ssize_t, [] { return options.huge_page_allocator_options.experimental_max_purge_num_huge_pages; }>(
        "experimental_hpa_max_purge_nhp"),
    readOnlyLeaf<size_t, [] { return options.huge_page_allocator_options.purge_threshold; }>("hpa_purge_threshold"),
    readOnlyLeaf<uint64_t, [] { return options.huge_page_allocator_options.min_purge_delay_ms; }>("hpa_min_purge_delay_ms"),
    readOnlyLeaf<
        const char *,
        [] { return huge_page_allocator_hugify_style_names[unsigned(options.huge_page_allocator_options.hugify_style)]; }>(
        "hpa_hugify_style"),
    /// `fxp_t`.
    readOnlyLeaf<FixedPoint, [] { return options.huge_page_allocator_options.dirty_multiplier; }>("hpa_dirty_mult"),
    readOnlyLeaf<size_t, [] { return options.small_extent_cache_options.num_shards; }>("hpa_sec_nshards"),
    readOnlyLeaf<size_t, [] { return options.small_extent_cache_options.max_alloc; }>("hpa_sec_max_alloc"),
    readOnlyLeaf<size_t, [] { return options.small_extent_cache_options.max_bytes; }>("hpa_sec_max_bytes"),
    readOnlyLeaf<size_t, [] { return options.small_extent_cache_options.batch_fill_extra; }>("hpa_sec_batch_fill_extra"),
    readOnlyLeaf<bool, [] { return options.huge_arena_transparent_huge_pages; }>("huge_arena_pac_thp"),
    readOnlyLeaf<
        const char *,
        [] { return metadata_transparent_huge_pages_mode_names[unsigned(options.metadata_transparent_huge_pages)]; }>("metadata_thp"),
    readOnlyLeaf<bool, [] { return options.retain; }>("retain"),
    readOnlyLeaf<const char *, [] { return options.sbrk; }>("dss"),
    readOnlyLeaf<unsigned, [] { return options.num_arenas; }>("narenas"),
    readOnlyLeaf<const char *, [] { return per_cpu_arena_mode_names[unsigned(options.per_cpu_arena)]; }>("percpu_arena"),
    readOnlyLeaf<size_t, [] { return options.oversize_threshold; }>("oversize_threshold"),
    readOnlyLeaf<int64_t, [] { return options.mutex_max_spin; }>("mutex_max_spin"),
    readOnlyLeaf<bool, [] { return options.background_thread; }>("background_thread"),
    readOnlyLeaf<size_t, [] { return options.max_background_threads; }>("max_background_threads"),
    readOnlyLeaf<ssize_t, [] { return options.dirty_decay_ms; }>("dirty_decay_ms"),
    readOnlyLeaf<ssize_t, [] { return options.muzzy_decay_ms; }>("muzzy_decay_ms"),
    readOnlyLeaf<bool, [] { return options.stats_print; }>("stats_print"),
    readOnlyLeaf<const char *, [] { return static_cast<const char *>(options.stats_print_options); }>("stats_print_opts"),
    readOnlyLeaf<int64_t, [] { return options.stats_interval; }>("stats_interval"),
    readOnlyLeaf<const char *, [] { return static_cast<const char *>(options.stats_interval_options); }>("stats_interval_opts"),
    readOnlyLeafIf<const char *, configFill, [] { return options.junk; }>("junk"),
    readOnlyLeafIf<bool, configFill, [] { return options.zero; }>("zero"),
    /// jemalloc: config_utrace, config_xmalloc are false.
    readOnlyLeafIf<bool, [] { return false; }, [] { return options.utrace; }>("utrace"),
    readOnlyLeafIf<bool, [] { return false; }, [] { return options.abort_on_out_of_memory; }>("xmalloc"),
    readOnlyLeafIf<bool, [] { return config::enable_cxx; }, [] { return options.experimental_infallible_new; }>(
        "experimental_infallible_new"),
    readOnlyLeaf<bool, [] { return options.experimental_thread_cache_gc; }>("experimental_tcache_gc"),
    readOnlyLeaf<bool, [] { return options.thread_cache; }>("tcache"),
    readOnlyLeaf<size_t, [] { return options.thread_cache_max; }>("tcache_max"),
    readOnlyLeaf<unsigned, [] { return options.thread_cache_num_slots_small_min; }>("tcache_nslots_small_min"),
    readOnlyLeaf<unsigned, [] { return options.thread_cache_num_slots_small_max; }>("tcache_nslots_small_max"),
    readOnlyLeaf<unsigned, [] { return options.thread_cache_num_slots_large; }>("tcache_nslots_large"),
    readOnlyLeaf<ssize_t, [] { return options.log2_thread_cache_num_slots_multiplier; }>("lg_tcache_nslots_mul"),
    readOnlyLeaf<size_t, [] { return options.thread_cache_gc_increment_bytes; }>("tcache_gc_incr_bytes"),
    readOnlyLeaf<size_t, [] { return options.thread_cache_gc_delay_bytes; }>("tcache_gc_delay_bytes"),
    readOnlyLeaf<unsigned, [] { return options.log2_thread_cache_flush_small_division; }>("lg_tcache_flush_small_div"),
    readOnlyLeaf<unsigned, [] { return options.log2_thread_cache_flush_large_division; }>("lg_tcache_flush_large_div"),
    readOnlyLeaf<const char *, [] { return transparent_huge_pages_mode_names[unsigned(options.transparent_huge_pages)]; }>("thp"),
    readOnlyLeaf<size_t, [] { return options.log2_extent_max_active_fit; }>("lg_extent_max_active_fit"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling; }>("prof"),
    readOnlyLeafIf<const char *, configProfiling, [] { return static_cast<const char *>(options.profiling_prefix); }>("prof_prefix"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_active; }>("prof_active"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_thread_active_init; }>("prof_thread_active_init"),
    readOnlyLeafIf<unsigned, configProfiling, [] { return options.profiling_backtrace_max; }>("prof_bt_max"),
    readOnlyLeafIf<size_t, configProfiling, [] { return options.log2_profiling_sample; }>("lg_prof_sample"),
    readOnlyLeafIf<ssize_t, configProfiling, [] { return options.log2_profiling_interval; }>("lg_prof_interval"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_growth_dump; }>("prof_gdump"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_final; }>("prof_final"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_leak; }>("prof_leak"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_leak_error; }>("prof_leak_error"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_accumulated; }>("prof_accum"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_pid_namespace; }>("prof_pid_namespace"),
    readOnlyLeafIf<ssize_t, configProfiling, [] { return options.profiling_recent_alloc_max; }>("prof_recent_alloc_max"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_stats; }>("prof_stats"),
    readOnlyLeafIf<bool, configProfiling, [] { return options.profiling_system_thread_name; }>("prof_sys_thread_name"),
    readOnlyLeafIf<
        const char *,
        configProfiling,
        [] { return profiling_time_resolution_mode_names[unsigned(options.profiling_time_resolution)]; }>("prof_time_resolution"),
    readOnlyLeafIf<ssize_t, [] { return config::use_after_free_detection; }, [] { return options.log2_sanitizer_use_after_free_align; }>(
        "lg_san_uaf_align"),
    readOnlyLeaf<const char *, [] { return zero_realloc_mode_names[unsigned(options.zero_realloc_action)]; }>("zero_realloc"),
    readOnlyLeaf<unsigned, [] { return options.debug_double_free_max_scan; }>("debug_double_free_max_scan"),
    readOnlyLeaf<bool, [] { return options.disable_large_size_classes; }>("disable_large_size_classes"),
    readOnlyLeaf<size_t, [] { return options.process_madvise_max_batch; }>("process_madvise_max_batch"),
    named("malloc_conf", option_malloc_conf_node),
};

/// --- tcache, arena, arenas -------------------------------------------------------------------------------------------

constexpr MallctlNode thread_cache_node[] = {
    leaf("create", mallctl::threadCacheCreate),
    leaf("flush", mallctl::threadCacheFlush),
    leaf("destroy", mallctl::threadCacheDestroy),
};

constexpr MallctlNode arena_i_node[] = {
    leaf("initialized", mallctl::arenaIInitialized),
    leaf("decay", mallctl::arenaIDecay),
    leaf("purge", mallctl::arenaIPurge),
    leaf("reset", mallctl::arenaIReset),
    leaf("destroy", mallctl::arenaIDestroy),
    leaf("dss", mallctl::arenaISbrk),
    /// Undocumented for now, since we anticipate an arena API in flux after we cut the last 5-series release.
    leaf("oversize_threshold", mallctl::arenaIOversizeThreshold),
    leaf("dirty_decay_ms", mallctl::arenaIDirtyDecayMs),
    leaf("muzzy_decay_ms", mallctl::arenaIMuzzyDecayMs),
    leaf("extent_hooks", mallctl::arenaIExtentHooks),
    leaf("retain_grow_limit", mallctl::arenaIRetainGrowLimit),
    leaf("name", mallctl::arenaIName),
};
constexpr MallctlNode super_arena_i_node[] = {super(arena_i_node)};

constexpr MallctlNode arenas_bin_i_node[] = {
    readOnlyLeaf<size_t, [](const size_t * numeric_path) { return binInfoOfNumericPath(numeric_path).region_size; }>("size"),
    readOnlyLeaf<uint32_t, [](const size_t * numeric_path) { return binInfoOfNumericPath(numeric_path).num_regions; }>("nregs"),
    readOnlyLeaf<size_t, [](const size_t * numeric_path) { return binInfoOfNumericPath(numeric_path).slab_size; }>("slab_size"),
    readOnlyLeaf<uint32_t, [](const size_t * numeric_path) { return binInfoOfNumericPath(numeric_path).num_shards; }>("nshards"),
};
constexpr MallctlNode super_arenas_bin_i_node[] = {super(arenas_bin_i_node)};

constexpr MallctlNode arenas_large_extent_i_node[] = {
    readOnlyLeaf<size_t, [](const size_t * numeric_path) { return largeExtentSizeOfNumericPath(numeric_path); }>("size"),
};
constexpr MallctlNode super_arenas_large_extent_i_node[] = {super(arenas_large_extent_i_node)};

constexpr MallctlNode arenas_node[] = {
    leaf("narenas", mallctl::arenasNumArenas),
    leaf("dirty_decay_ms", mallctl::arenasDirtyDecayMs),
    leaf("muzzy_decay_ms", mallctl::arenasMuzzyDecayMs),
    readOnlyLeaf<size_t, [] { return QUANTUM; }>("quantum"),
    readOnlyLeaf<size_t, [] { return PAGE; }>("page"),
    readOnlyLeaf<size_t, [] { return HUGE_PAGE; }>("hugepage"),
    leaf("tcache_max", mallctl::arenasThreadCacheMax),
    readOnlyLeaf<unsigned, [] { return SIZE_CLASS_NUM_BINS; }>("nbins"),
    leaf("nhbins", mallctl::arenasNumThreadCacheBins),
    indexed("bin", mallctl::arenasBinIIndex, super_arenas_bin_i_node),
    readOnlyLeaf<unsigned, [] { return SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS; }>("nlextents"),
    indexed("lextent", mallctl::arenasLargeExtentIIndex, super_arenas_large_extent_i_node),
    leaf("create", mallctl::arenasCreate),
    leaf("lookup", mallctl::arenasLookup),
};

/// --- prof ------------------------------------------------------------------------------------------------------------

constexpr MallctlNode profiling_stats_bins_i_node[] = {
    leaf("live", mallctl::profilingStatsBinsILive),
    leaf("accum", mallctl::profilingStatsBinsIAccumulated),
};
constexpr MallctlNode super_profiling_stats_bins_i_node[] = {super(profiling_stats_bins_i_node)};

constexpr MallctlNode profiling_stats_large_extents_i_node[] = {
    leaf("live", mallctl::profilingStatsLargeExtentsILive),
    leaf("accum", mallctl::profilingStatsLargeExtentsIAccumulated),
};
constexpr MallctlNode super_profiling_stats_large_extents_i_node[] = {super(profiling_stats_large_extents_i_node)};

constexpr MallctlNode profiling_stats_node[] = {
    indexed("bins", mallctl::profilingStatsBinsIIndex, super_profiling_stats_bins_i_node),
    indexed("lextents", mallctl::profilingStatsLargeExtentsIIndex, super_profiling_stats_large_extents_i_node),
};

constexpr MallctlNode profiling_node[] = {
    leaf("thread_active_init", mallctl::profilingThreadActiveInit),
    leaf("active", mallctl::profilingActive),
    leaf("dump", mallctl::profilingDump),
    leaf("gdump", mallctl::profilingGrowthDump),
    leaf("prefix", mallctl::profilingPrefix),
    leaf("reset", mallctl::profilingReset),
    leaf("interval", mallctl::profilingInterval),
    leaf("lg_sample", mallctl::profilingLog2Sample),
    leaf("log_start", mallctl::profilingLogStart),
    leaf("log_stop", mallctl::profilingLogStop),
    named("stats", profiling_stats_node),
};

/// --- stats.arenas.<i> ------------------------------------------------------------------------------------------------

constexpr MallctlNode stats_arenas_i_small_node[] = {
    leaf("allocated", mallctl::statsArenasISmallAllocated),
    leaf("nmalloc", mallctl::statsArenasISmallNumAllocations),
    leaf("ndalloc", mallctl::statsArenasISmallNumDeallocations),
    leaf("nrequests", mallctl::statsArenasISmallNumRequests),
    leaf("nfills", mallctl::statsArenasISmallNumFills),
    leaf("nflushes", mallctl::statsArenasISmallNumFlushes),
};

constexpr MallctlNode stats_arenas_i_large_node[] = {
    leaf("allocated", mallctl::statsArenasILargeAllocated),
    leaf("nmalloc", mallctl::statsArenasILargeNumAllocations),
    leaf("ndalloc", mallctl::statsArenasILargeNumDeallocations),
    leaf("nrequests", mallctl::statsArenasILargeNumRequests),
    leaf("nfills", mallctl::statsArenasILargeNumFills),
    leaf("nflushes", mallctl::statsArenasILargeNumFlushes),
};

constexpr MallctlNode stats_arenas_i_bins_j_node[] = {
    leaf("nmalloc", mallctl::statsArenasIBinsJNumAllocations),
    leaf("ndalloc", mallctl::statsArenasIBinsJNumDeallocations),
    leaf("nrequests", mallctl::statsArenasIBinsJNumRequests),
    leaf("curregs", mallctl::statsArenasIBinsJCurrentRegions),
    leaf("nfills", mallctl::statsArenasIBinsJNumFills),
    leaf("nflushes", mallctl::statsArenasIBinsJNumFlushes),
    leaf("nslabs", mallctl::statsArenasIBinsJNumSlabs),
    leaf("nreslabs", mallctl::statsArenasIBinsJNumSlabChanges),
    leaf("curslabs", mallctl::statsArenasIBinsJCurrentSlabs),
    leaf("nonfull_slabs", mallctl::statsArenasIBinsJNonFullSlabs),
    named("mutex", mutex_profiling_node<&mallctl::binMutexProfilingData>),
};
constexpr MallctlNode super_stats_arenas_i_bins_j_node[] = {super(stats_arenas_i_bins_j_node)};

constexpr MallctlNode stats_arenas_i_large_extents_j_node[] = {
    leaf("nmalloc", mallctl::statsArenasILargeExtentsJNumAllocations),
    leaf("ndalloc", mallctl::statsArenasILargeExtentsJNumDeallocations),
    leaf("nrequests", mallctl::statsArenasILargeExtentsJNumRequests),
    leaf("curlextents", mallctl::statsArenasILargeExtentsJCurrentLargeExtents),
};
constexpr MallctlNode super_stats_arenas_i_large_extents_j_node[] = {super(stats_arenas_i_large_extents_j_node)};

constexpr MallctlNode stats_arenas_i_extents_j_node[] = {
    leaf("ndirty", mallctl::statsArenasIExtentsJNumDirty),
    leaf("nmuzzy", mallctl::statsArenasIExtentsJNumMuzzy),
    leaf("nretained", mallctl::statsArenasIExtentsJNumRetained),
    leaf("dirty_bytes", mallctl::statsArenasIExtentsJDirtyBytes),
    leaf("muzzy_bytes", mallctl::statsArenasIExtentsJMuzzyBytes),
    leaf("retained_bytes", mallctl::statsArenasIExtentsJRetainedBytes),
};
constexpr MallctlNode super_stats_arenas_i_extents_j_node[] = {super(stats_arenas_i_extents_j_node)};

/// jemalloc: MUTEX_PROF_ARENA_MUTEXES
constexpr MallctlNode stats_arenas_i_mutexes_node[] = {
    named("large", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_large>>),
    named("extent_avail", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_extent_available>>),
    named("extents_dirty", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_extents_dirty>>),
    named("extents_muzzy", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_extents_muzzy>>),
    named("extents_retained", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_extents_retained>>),
    named("decay_dirty", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_decay_dirty>>),
    named("decay_muzzy", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_decay_muzzy>>),
    named("base", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_base>>),
    named("tcache_list", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_thread_cache_list>>),
    named("hpa_shard", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_huge_page_shard>>),
    named("hpa_shard_grow", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_huge_page_shard_grow>>),
    named("hpa_sec", mutex_profiling_node<&mallctl::arenaMutexProfilingDataOf<arena_profiling_mutex_small_extent_cache>>),
};

/// HPA is dropped: all its statistics are zero (in jemalloc too, since HPA is never enabled in ClickHouse). The
/// same children are used for `slabs`, `full_slabs`, `empty_slabs` and `nonfull_slabs.<j>`.
constexpr MallctlNode stats_arenas_i_huge_page_shard_slabs_node[] = {
    zeroStat<size_t>("npageslabs_nonhuge"),
    zeroStat<size_t>("npageslabs_huge"),
    zeroStat<size_t>("nactive_nonhuge"),
    zeroStat<size_t>("nactive_huge"),
    zeroStat<size_t>("ndirty_nonhuge"),
    zeroStat<size_t>("ndirty_huge"),
};
constexpr MallctlNode super_stats_arenas_i_huge_page_shard_non_full_slabs_j_node[] = {super(stats_arenas_i_huge_page_shard_slabs_node)};

constexpr MallctlNode stats_arenas_i_huge_page_shard_node[] = {
    zeroStat<size_t>("npageslabs"),
    zeroStat<size_t>("nactive"),
    zeroStat<size_t>("ndirty"),

    named("slabs", stats_arenas_i_huge_page_shard_slabs_node),

    zeroStat<uint64_t>("npurge_passes"),
    zeroStat<uint64_t>("npurges"),
    zeroStat<uint64_t>("nhugifies"),
    zeroStat<uint64_t>("nhugify_failures"),
    zeroStat<uint64_t>("ndehugifies"),

    named("full_slabs", stats_arenas_i_huge_page_shard_slabs_node),
    named("empty_slabs", stats_arenas_i_huge_page_shard_slabs_node),
    indexed(
        "nonfull_slabs", mallctl::statsArenasIHugePageShardNonFullSlabsJIndex, super_stats_arenas_i_huge_page_shard_non_full_slabs_j_node),
};

constexpr MallctlNode stats_arenas_i_node[] = {
    leaf("nthreads", mallctl::statsArenasINumThreads),
    leaf("uptime", mallctl::statsArenasIUptime),
    leaf("dss", mallctl::statsArenasISbrk),
    leaf("dirty_decay_ms", mallctl::statsArenasIDirtyDecayMs),
    leaf("muzzy_decay_ms", mallctl::statsArenasIMuzzyDecayMs),
    leaf("pactive", mallctl::statsArenasIActivePages),
    leaf("pdirty", mallctl::statsArenasIDirtyPages),
    leaf("pmuzzy", mallctl::statsArenasIMuzzyPages),
    leaf("mapped", mallctl::statsArenasIMapped),
    leaf("retained", mallctl::statsArenasIRetained),
    leaf("extent_avail", mallctl::statsArenasIExtentAvailable),
    leaf("dirty_npurge", mallctl::statsArenasIDirtyNumPurge),
    leaf("dirty_nmadvise", mallctl::statsArenasIDirtyNumMadvises),
    leaf("dirty_purged", mallctl::statsArenasIDirtyPurged),
    leaf("muzzy_npurge", mallctl::statsArenasIMuzzyNumPurge),
    leaf("muzzy_nmadvise", mallctl::statsArenasIMuzzyNumMadvises),
    leaf("muzzy_purged", mallctl::statsArenasIMuzzyPurged),
    leaf("base", mallctl::statsArenasIBase),
    leaf("internal", mallctl::statsArenasIInternal),
    leaf("metadata_edata", mallctl::statsArenasIMetadataExtent),
    leaf("metadata_rtree", mallctl::statsArenasIMetadataRadixTree),
    leaf("metadata_thp", mallctl::statsArenasIMetadataTransparentHugePages),
    leaf("tcache_bytes", mallctl::statsArenasIThreadCacheBytes),
    leaf("tcache_stashed_bytes", mallctl::statsArenasIThreadCacheStashedBytes),
    leaf("resident", mallctl::statsArenasIResident),
    leaf("abandoned_vm", mallctl::statsArenasIAbandonedVM),
    /// SEC is dropped: zero (in jemalloc too). Note the order: `noflush` before `flush`.
    zeroStat<size_t>("hpa_sec_bytes"),
    zeroStat<size_t>("hpa_sec_hits"),
    zeroStat<size_t>("hpa_sec_misses"),
    zeroStat<size_t>("hpa_sec_dalloc_noflush"),
    zeroStat<size_t>("hpa_sec_dalloc_flush"),
    zeroStat<size_t>("hpa_sec_overfills"),
    named("small", stats_arenas_i_small_node),
    named("large", stats_arenas_i_large_node),
    indexed("bins", mallctl::statsArenasIBinsJIndex, super_stats_arenas_i_bins_j_node),
    indexed("lextents", mallctl::statsArenasILargeExtentsJIndex, super_stats_arenas_i_large_extents_j_node),
    indexed("extents", mallctl::statsArenasIExtentsJIndex, super_stats_arenas_i_extents_j_node),
    named("mutexes", stats_arenas_i_mutexes_node),
    named("hpa_shard", stats_arenas_i_huge_page_shard_node),
};
constexpr MallctlNode super_stats_arenas_i_node[] = {super(stats_arenas_i_node)};

/// --- stats -----------------------------------------------------------------------------------------------------------

constexpr MallctlNode stats_background_thread_node[] = {
    leaf("num_threads", mallctl::statsBackgroundThreadNumThreads),
    leaf("num_runs", mallctl::statsBackgroundThreadNumRuns),
    leaf("run_interval", mallctl::statsBackgroundThreadRunInterval),
};

/// jemalloc: MUTEX_PROF_GLOBAL_MUTEXES
constexpr MallctlNode stats_mutexes_node[] = {
    named("background_thread", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_background_thread>>),
    named("max_per_bg_thd", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_max_per_background_thread>>),
    named("ctl", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_mallctl>>),
    named("prof", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling>>),
    named("prof_thds_data", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling_threads_data>>),
    named("prof_dump", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling_dump>>),
    named("prof_recent_alloc", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling_recent_alloc>>),
    named("prof_recent_dump", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling_recent_dump>>),
    named("prof_stats", mutex_profiling_node<&mallctl::globalMutexProfilingData<global_profiling_mutex_profiling_stats>>),
    leaf("reset", mallctl::statsMutexesReset),
};

constexpr MallctlNode approximate_stats_node[] = {
    leaf("active", mallctl::approximateStatsActive),
};

constexpr MallctlNode stats_node[] = {
    leaf("allocated", mallctl::statsAllocated),
    leaf("active", mallctl::statsActive),
    leaf("metadata", mallctl::statsMetadata),
    leaf("metadata_edata", mallctl::statsMetadataExtent),
    leaf("metadata_rtree", mallctl::statsMetadataRadixTree),
    leaf("metadata_thp", mallctl::statsMetadataTransparentHugePages),
    leaf("resident", mallctl::statsResident),
    leaf("mapped", mallctl::statsMapped),
    leaf("retained", mallctl::statsRetained),
    named("background_thread", stats_background_thread_node),
    named("mutexes", stats_mutexes_node),
    indexed("arenas", mallctl::statsArenasIIndex, super_stats_arenas_i_node),
    leaf("zero_reallocs", mallctl::statsZeroReallocs),
};

/// --- experimental ----------------------------------------------------------------------------------------------------

constexpr MallctlNode experimental_hooks_node[] = {
    leaf("install", mallctl::experimentalHooksInstall),
    leaf("remove", mallctl::experimentalHooksRemove),
    leaf("prof_backtrace", mallctl::experimentalHooksProfilingBacktrace),
    leaf("prof_dump", mallctl::experimentalHooksProfilingDump),
    leaf("prof_sample", mallctl::experimentalHooksProfilingSample),
    leaf("prof_sample_free", mallctl::experimentalHooksProfilingSampleFree),
    leaf("safety_check_abort", mallctl::experimentalHooksSafetyCheckAbort),
    leaf("thread_event", mallctl::experimentalHooksThreadEvent),
};

constexpr MallctlNode experimental_thread_node[] = {
    leaf("activity_callback", mallctl::experimentalThreadActivityCallback),
};

constexpr MallctlNode experimental_utilization_node[] = {
    leaf("query", mallctl::experimentalUtilizationQuery),
    leaf("batch_query", mallctl::experimentalUtilizationBatchQuery),
};

constexpr MallctlNode experimental_arenas_i_node[] = {
    leaf("pactivep", mallctl::experimentalArenasIActivePagesPtr),
};
constexpr MallctlNode super_experimental_arenas_i_node[] = {super(experimental_arenas_i_node)};

constexpr MallctlNode experimental_profiling_recent_node[] = {
    leaf("alloc_max", mallctl::experimentalProfilingRecentAllocMax),
    leaf("alloc_dump", mallctl::experimentalProfilingRecentAllocDump),
};

constexpr MallctlNode experimental_node[] = {
    named("hooks", experimental_hooks_node),
    named("utilization", experimental_utilization_node),
    indexed("arenas", mallctl::experimentalArenasIIndex, super_experimental_arenas_i_node),
    leaf("arenas_create_ext", mallctl::experimentalArenasCreateExtended),
    named("prof_recent", experimental_profiling_recent_node),
    leaf("batch_alloc", mallctl::experimentalBatchAlloc),
    named("thread", experimental_thread_node),
};

/// --- root ------------------------------------------------------------------------------------------------------------

constexpr MallctlNode root_node[] = {
    leaf("version", mallctl::version),
    leaf("epoch", mallctl::epoch),
    leaf("background_thread", mallctl::backgroundThread),
    leaf("max_background_threads", mallctl::maxBackgroundThreads),
    named("thread", thread_node),
    named("config", config_node),
    named("opt", option_node),
    named("tcache", thread_cache_node),
    indexed("arena", mallctl::arenaIIndex, super_arena_i_node),
    named("arenas", arenas_node),
    named("prof", profiling_node),
    named("stats", stats_node),
    named("approximate_stats", approximate_stats_node),
    named("experimental", experimental_node),
};

/// --- Compile-time checks of the tree -------------------------------------------------------------------------------

constexpr bool sameName(const char * a, const char * b)
{
    while (*a != '\0' && *a == *b)
    {
        ++a;
        ++b;
    }
    return *a == *b;
}

/// Every leaf has no children and every inner node has some; names are non-empty and unique among siblings;
/// indexed levels have exactly one (super) child with an empty name; the depth (counting indexed levels) is at most
/// `MALLCTL_MAX_DEPTH`.
constexpr bool checkTree(const MallctlNode & node, size_t depth)
{
    if (depth > MALLCTL_MAX_DEPTH)
        return false;
    if (node.isLeaf())
        return node.children == nullptr && node.num_children == 0 && node.index == nullptr;
    if (node.children == nullptr || node.num_children == 0)
        return false;
    if (node.isIndexed())
    {
        const MallctlNode & super_node = node.children[0];
        if (node.num_children != 1 || super_node.name[0] != '\0' || super_node.isLeaf() || super_node.isIndexed())
            return false;
        for (size_t i = 0; i < super_node.num_children; ++i)
            if (!checkTree(super_node.children[i], depth + 2))
                return false;
        return true;
    }
    for (size_t i = 0; i < node.num_children; ++i)
    {
        if (node.children[i].name[0] == '\0')
            return false;
        for (size_t j = 0; j < i; ++j)
            if (sameName(node.children[i].name, node.children[j].name))
                return false;
        if (!checkTree(node.children[i], depth + 1))
            return false;
    }
    return true;
}

constexpr MallctlNode root_check_node = super(root_node);
static_assert(checkTree(root_check_node, 0));

}

constinit const MallctlNode mallctl_super_root_node[1] = {super(root_node)};

}
