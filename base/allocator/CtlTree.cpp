/// The `mallctl` tree (jemalloc: `ctl.c`, the `*_node` arrays).
///
/// The children of every node are listed in exactly jemalloc's order: MIB components are positions in these arrays.
/// The leaves whose value is a constant, an option or a configuration flag (`config.*`, `opt.*`, most of `arenas.*`),
/// the leaves of the dropped HPA/SEC statistics (always zero) and the mutex profiling leaves are generated here from
/// the templates of CtlImpl.h; all other leaves are declared in CtlImpl.h and defined in the file of their subtree.

#include <allocator/Ctl.h>

#include <allocator/CtlImpl.h>
#include <allocator/ExtentHooks.h>
#include <allocator/NsTime.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include <cstddef>

/// Defined in Conf.cpp. jemalloc: je_malloc_conf, je_malloc_conf_2_conf_harder
extern "C" const char * je_malloc_conf;
extern "C" const char * je_malloc_conf_2_conf_harder;

namespace jemalloc
{

namespace
{

using ctl::MutexProfCounter;

/// --- Node constructors ---------------------------------------------------------------------------------------------

/// jemalloc: {NAME(n), CTL(c)}
constexpr CtlNode leaf(const char * name, CtlLeafFn fn)
{
    return {name, nullptr, 0, nullptr, fn};
}

/// jemalloc: {NAME(n), CHILD(named, c)}
template <size_t N>
constexpr CtlNode named(const char * name, const CtlNode (&children)[N])
{
    return {name, children, N, nullptr, nullptr};
}

/// jemalloc: {NAME(n), CHILD(indexed, c)} with `c_node[] = {{INDEX(i)}}`; `super` is the node returned by the
/// index function (`super_i_node`).
constexpr CtlNode indexed(const char * name, CtlIndexFn index, const CtlNode (&super)[1])
{
    return {name, super, 1, index, nullptr};
}

/// jemalloc: super_*_node[] = {{NAME(""), CHILD(named, *)}}
template <size_t N>
constexpr CtlNode super(const CtlNode (&children)[N])
{
    return {"", children, N, nullptr, nullptr};
}

/// jemalloc: CTL_RO_NL_GEN, CTL_RO_CONFIG_GEN
template <typename T, auto get>
constexpr CtlNode ro(const char * name)
{
    return leaf(name, &ctl::readOnlyNl<T, get>);
}

/// jemalloc: CTL_RO_NL_CGEN
template <typename T, auto cond, auto get>
constexpr CtlNode roIf(const char * name)
{
    return leaf(name, &ctl::readOnlyNlIf<T, cond, get>);
}

constexpr bool statsEnabled()
{
    return config::stats;
}

/// jemalloc: CTL_RO_CGEN(config_stats, ...)
template <typename T, auto get>
constexpr CtlNode roStats(const char * name)
{
    return leaf(name, &ctl::readOnlyLockedIf<T, statsEnabled, get>);
}

/// A statistic of a dropped feature (HPA, SEC): `CTL_RO_CGEN(config_stats, ...)` of a value that is always zero.
template <typename T>
constexpr CtlNode zeroStat(const char * name)
{
    return roStats<T, [] { return T(0); }>(name);
}

/// jemalloc: MUTEX_PROF_DATA_NODE
template <auto accessor>
constexpr CtlNode mutex_prof_node[] = {
    leaf("num_ops", &ctl::mutexProf<accessor, MutexProfCounter::NumOps>),
    leaf("num_wait", &ctl::mutexProf<accessor, MutexProfCounter::NumWait>),
    leaf("num_spin_acq", &ctl::mutexProf<accessor, MutexProfCounter::NumSpinAcq>),
    leaf("num_owner_switch", &ctl::mutexProf<accessor, MutexProfCounter::NumOwnerSwitch>),
    leaf("total_wait_time", &ctl::mutexProf<accessor, MutexProfCounter::TotalWaitTime>),
    leaf("max_wait_time", &ctl::mutexProf<accessor, MutexProfCounter::MaxWaitTime>),
    /// Note that # of current waiting thread not provided.
    leaf("max_num_thds", &ctl::mutexProf<accessor, MutexProfCounter::MaxNumThds>),
};

/// --- Values of the generated leaves --------------------------------------------------------------------------------

/// jemalloc: `bin_infos[mib[2]]`. The index function accepts `SC_NBINS` (off by one), where jemalloc reads past the
/// array; that bin reads as zeros here.
/// jemalloc compatibility: arenas_bin_i_index accepts i == SC_NBINS.
const BinInfo & binInfoOfMib(const size_t * mib)
{
    static constexpr BinInfo past_the_end{};
    return mib[2] < SC_NBINS ? bin_infos[mib[2]] : past_the_end;
}

/// jemalloc: `sz_index2size_unsafe(SC_NBINS + mib[2])`. The index function accepts `SC_NSIZES - SC_NBINS` (off by
/// one), where jemalloc reads past `sz_index2size_tab`; that size reads as 0 here.
/// jemalloc compatibility: arenas_lextent_i_index accepts i == SC_NSIZES - SC_NBINS.
size_t lextentSizeOfMib(const size_t * mib)
{
    return mib[2] < SC_NSIZES - SC_NBINS ? sz::indexToSizeUnsafe(SC_NBINS + static_cast<szind_t>(mib[2])) : 0;
}

}

/// --- Constant index functions ----------------------------------------------------------------------------------------

namespace ctl
{

/// jemalloc compatibility: `i > SC_NBINS` (not `>=`) is rejected, as in jemalloc.
/// jemalloc: arenas_bin_i_index
bool arenasBinIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    return !(i > SC_NBINS);
}

/// jemalloc compatibility: `i > SC_NSIZES - SC_NBINS` (not `>=`) is rejected, as in jemalloc.
/// jemalloc: arenas_lextent_i_index
bool arenasLextentIIndex(ThreadState *, const size_t *, size_t, size_t i)
{
    return !(i > SC_NSIZES - SC_NBINS);
}

/// jemalloc compatibility: `j > SC_NBINS` (not `>=`) is rejected, as in jemalloc (where `bins.<SC_NBINS>` reads
/// the beginning of `lstats`).
/// jemalloc: stats_arenas_i_bins_j_index
bool statsArenasIBinsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return !(j > SC_NBINS);
}

/// jemalloc compatibility: `j > SC_NSIZES - SC_NBINS` (not `>=`) is rejected, as in jemalloc (where
/// `lextents.<SC_NSIZES - SC_NBINS>` reads the beginning of `estats`).
/// jemalloc: stats_arenas_i_lextents_j_index
bool statsArenasILextentsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return !(j > SC_NSIZES - SC_NBINS);
}

/// jemalloc: stats_arenas_i_extents_j_index
bool statsArenasIExtentsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return j < SC_NPSIZES;
}

/// jemalloc: PSSET_NPSIZES
inline constexpr size_t PSSET_NPSIZES = 64;

/// jemalloc: stats_arenas_i_hpa_shard_nonfull_slabs_j_index
bool statsArenasIHpaShardNonfullSlabsJIndex(ThreadState *, const size_t *, size_t, size_t j)
{
    return j < PSSET_NPSIZES;
}

}

namespace
{

/// --- thread ----------------------------------------------------------------------------------------------------------

constexpr CtlNode thread_tcache_ncached_max_node[] = {
    leaf("read_sizeclass", ctl::threadTcacheNcachedMaxReadSizeclass),
    leaf("write", ctl::threadTcacheNcachedMaxWrite),
};

constexpr CtlNode thread_tcache_node[] = {
    leaf("enabled", ctl::threadTcacheEnabled),
    leaf("max", ctl::threadTcacheMax),
    leaf("flush", ctl::threadTcacheFlush),
    named("ncached_max", thread_tcache_ncached_max_node),
};

constexpr CtlNode thread_peak_node[] = {
    leaf("read", ctl::threadPeakRead),
    leaf("reset", ctl::threadPeakReset),
};

constexpr CtlNode thread_prof_node[] = {
    leaf("name", ctl::threadProfName),
    leaf("active", ctl::threadProfActive),
};

constexpr CtlNode thread_node[] = {
    leaf("arena", ctl::threadArena),
    leaf("allocated", ctl::threadAllocated),
    leaf("allocatedp", ctl::threadAllocatedp),
    leaf("deallocated", ctl::threadDeallocated),
    leaf("deallocatedp", ctl::threadDeallocatedp),
    named("tcache", thread_tcache_node),
    named("peak", thread_peak_node),
    named("prof", thread_prof_node),
    leaf("idle", ctl::threadIdle),
};

/// --- config (jemalloc: CTL_RO_CONFIG_GEN) ----------------------------------------------------------------------------

constexpr CtlNode config_node[] = {
    ro<bool, [] { return config::cache_oblivious; }>("cache_oblivious"),
    /// jemalloc's `config_debug` (`JEMALLOC_DEBUG`) is never enabled in ClickHouse; `ALLOCATOR_DEBUG` only enables
    /// our internal assertions.
    ro<bool, [] { return false; }>("debug"),
    ro<bool, [] { return config::fill; }>("fill"),
    ro<bool, [] { return config::lazy_lock; }>("lazy_lock"),
    ro<const char *, [] { return config::malloc_conf_default; }>("malloc_conf"),
    ro<bool, [] { return config::opt_safety_checks; }>("opt_safety_checks"),
    ro<bool, [] { return config::prof; }>("prof"),
    /// jemalloc: config_prof_libgcc (`JEMALLOC_PROF_LIBGCC`), config_prof_libunwind (`JEMALLOC_PROF_LIBUNWIND`),
    /// config_prof_frameptr (`JEMALLOC_PROF_FRAME_POINTER`): ClickHouse always uses libunwind.
    ro<bool, [] { return false; }>("prof_libgcc"),
    ro<bool, [] { return true; }>("prof_libunwind"),
    ro<bool, [] { return false; }>("prof_frameptr"),
    ro<bool, [] { return config::stats; }>("stats"),
    /// jemalloc: config_utrace (`JEMALLOC_UTRACE`), config_xmalloc (`JEMALLOC_XMALLOC`): never defined.
    ro<bool, [] { return false; }>("utrace"),
    ro<bool, [] { return false; }>("xmalloc"),
};

/// --- opt (jemalloc: CTL_RO_NL_GEN, CTL_RO_NL_CGEN) -------------------------------------------------------------------

constexpr bool configFill()
{
    return config::fill;
}

constexpr bool configProf()
{
    return config::prof;
}

constexpr CtlNode opt_malloc_conf_node[] = {
    roIf<const char *, [] { return opt.malloc_conf_symlink != nullptr; }, [] { return opt.malloc_conf_symlink; }>("symlink"),
    roIf<const char *, [] { return opt.malloc_conf_env_var != nullptr; }, [] { return opt.malloc_conf_env_var; }>("env_var"),
    roIf<const char *, [] { return je_malloc_conf != nullptr; }, [] { return je_malloc_conf; }>("global_var"),
    roIf<const char *, [] { return je_malloc_conf_2_conf_harder != nullptr; }, [] { return je_malloc_conf_2_conf_harder; }>(
        "global_var_2_conf_harder"),
};

constexpr CtlNode opt_node[] = {
    ro<bool, [] { return opt.abort; }>("abort"),
    ro<bool, [] { return opt.abort_conf; }>("abort_conf"),
    ro<bool, [] { return opt.cache_oblivious; }>("cache_oblivious"),
    ro<bool, [] { return opt.trust_madvise; }>("trust_madvise"),
    ro<bool, [] { return opt.experimental_hpa_start_huge_if_thp_always; }>("experimental_hpa_start_huge_if_thp_always"),
    ro<bool, [] { return opt.experimental_hpa_enforce_hugify; }>("experimental_hpa_enforce_hugify"),
    ro<bool, [] { return opt.confirm_conf; }>("confirm_conf"),
    ro<bool, [] { return opt.hpa; }>("hpa"),
    ro<size_t, [] { return opt.hpa_opts.slab_max_alloc; }>("hpa_slab_max_alloc"),
    ro<size_t, [] { return opt.hpa_opts.hugification_threshold; }>("hpa_hugification_threshold"),
    ro<uint64_t, [] { return opt.hpa_opts.hugify_delay_ms; }>("hpa_hugify_delay_ms"),
    ro<bool, [] { return opt.hpa_opts.hugify_sync; }>("hpa_hugify_sync"),
    ro<uint64_t, [] { return opt.hpa_opts.min_purge_interval_ms; }>("hpa_min_purge_interval_ms"),
    ro<ssize_t, [] { return opt.hpa_opts.experimental_max_purge_nhp; }>("experimental_hpa_max_purge_nhp"),
    ro<size_t, [] { return opt.hpa_opts.purge_threshold; }>("hpa_purge_threshold"),
    ro<uint64_t, [] { return opt.hpa_opts.min_purge_delay_ms; }>("hpa_min_purge_delay_ms"),
    ro<const char *, [] { return hpa_hugify_style_names[unsigned(opt.hpa_opts.hugify_style)]; }>("hpa_hugify_style"),
    /// `fxp_t`.
    ro<FixedPoint, [] { return opt.hpa_opts.dirty_mult; }>("hpa_dirty_mult"),
    ro<size_t, [] { return opt.hpa_sec_opts.nshards; }>("hpa_sec_nshards"),
    ro<size_t, [] { return opt.hpa_sec_opts.max_alloc; }>("hpa_sec_max_alloc"),
    ro<size_t, [] { return opt.hpa_sec_opts.max_bytes; }>("hpa_sec_max_bytes"),
    ro<size_t, [] { return opt.hpa_sec_opts.batch_fill_extra; }>("hpa_sec_batch_fill_extra"),
    ro<bool, [] { return opt.huge_arena_pac_thp; }>("huge_arena_pac_thp"),
    ro<const char *, [] { return metadata_thp_mode_names[unsigned(opt.metadata_thp)]; }>("metadata_thp"),
    ro<bool, [] { return opt.retain; }>("retain"),
    ro<const char *, [] { return opt.dss; }>("dss"),
    ro<unsigned, [] { return opt.narenas; }>("narenas"),
    ro<const char *, [] { return percpu_arena_mode_names[unsigned(opt.percpu_arena)]; }>("percpu_arena"),
    ro<size_t, [] { return opt.oversize_threshold; }>("oversize_threshold"),
    ro<int64_t, [] { return opt.mutex_max_spin; }>("mutex_max_spin"),
    ro<bool, [] { return opt.background_thread; }>("background_thread"),
    ro<size_t, [] { return opt.max_background_threads; }>("max_background_threads"),
    ro<ssize_t, [] { return opt.dirty_decay_ms; }>("dirty_decay_ms"),
    ro<ssize_t, [] { return opt.muzzy_decay_ms; }>("muzzy_decay_ms"),
    ro<bool, [] { return opt.stats_print; }>("stats_print"),
    ro<const char *, [] { return static_cast<const char *>(opt.stats_print_opts); }>("stats_print_opts"),
    ro<int64_t, [] { return opt.stats_interval; }>("stats_interval"),
    ro<const char *, [] { return static_cast<const char *>(opt.stats_interval_opts); }>("stats_interval_opts"),
    roIf<const char *, configFill, [] { return opt.junk; }>("junk"),
    roIf<bool, configFill, [] { return opt.zero; }>("zero"),
    /// jemalloc: config_utrace, config_xmalloc are false.
    roIf<bool, [] { return false; }, [] { return opt.utrace; }>("utrace"),
    roIf<bool, [] { return false; }, [] { return opt.xmalloc; }>("xmalloc"),
    roIf<bool, [] { return config::enable_cxx; }, [] { return opt.experimental_infallible_new; }>("experimental_infallible_new"),
    ro<bool, [] { return opt.experimental_tcache_gc; }>("experimental_tcache_gc"),
    ro<bool, [] { return opt.tcache; }>("tcache"),
    ro<size_t, [] { return opt.tcache_max; }>("tcache_max"),
    ro<unsigned, [] { return opt.tcache_nslots_small_min; }>("tcache_nslots_small_min"),
    ro<unsigned, [] { return opt.tcache_nslots_small_max; }>("tcache_nslots_small_max"),
    ro<unsigned, [] { return opt.tcache_nslots_large; }>("tcache_nslots_large"),
    ro<ssize_t, [] { return opt.lg_tcache_nslots_mul; }>("lg_tcache_nslots_mul"),
    ro<size_t, [] { return opt.tcache_gc_incr_bytes; }>("tcache_gc_incr_bytes"),
    ro<size_t, [] { return opt.tcache_gc_delay_bytes; }>("tcache_gc_delay_bytes"),
    ro<unsigned, [] { return opt.lg_tcache_flush_small_div; }>("lg_tcache_flush_small_div"),
    ro<unsigned, [] { return opt.lg_tcache_flush_large_div; }>("lg_tcache_flush_large_div"),
    ro<const char *, [] { return thp_mode_names[unsigned(opt.thp)]; }>("thp"),
    ro<size_t, [] { return opt.lg_extent_max_active_fit; }>("lg_extent_max_active_fit"),
    roIf<bool, configProf, [] { return opt.prof; }>("prof"),
    roIf<const char *, configProf, [] { return static_cast<const char *>(opt.prof_prefix); }>("prof_prefix"),
    roIf<bool, configProf, [] { return opt.prof_active; }>("prof_active"),
    roIf<bool, configProf, [] { return opt.prof_thread_active_init; }>("prof_thread_active_init"),
    roIf<unsigned, configProf, [] { return opt.prof_bt_max; }>("prof_bt_max"),
    roIf<size_t, configProf, [] { return opt.lg_prof_sample; }>("lg_prof_sample"),
    roIf<ssize_t, configProf, [] { return opt.lg_prof_interval; }>("lg_prof_interval"),
    roIf<bool, configProf, [] { return opt.prof_gdump; }>("prof_gdump"),
    roIf<bool, configProf, [] { return opt.prof_final; }>("prof_final"),
    roIf<bool, configProf, [] { return opt.prof_leak; }>("prof_leak"),
    roIf<bool, configProf, [] { return opt.prof_leak_error; }>("prof_leak_error"),
    roIf<bool, configProf, [] { return opt.prof_accum; }>("prof_accum"),
    roIf<bool, configProf, [] { return opt.prof_pid_namespace; }>("prof_pid_namespace"),
    roIf<ssize_t, configProf, [] { return opt.prof_recent_alloc_max; }>("prof_recent_alloc_max"),
    roIf<bool, configProf, [] { return opt.prof_stats; }>("prof_stats"),
    roIf<bool, configProf, [] { return opt.prof_sys_thread_name; }>("prof_sys_thread_name"),
    roIf<const char *, configProf, [] { return prof_time_res_mode_names[unsigned(opt.prof_time_res)]; }>("prof_time_resolution"),
    roIf<ssize_t, [] { return config::uaf_detection; }, [] { return opt.lg_san_uaf_align; }>("lg_san_uaf_align"),
    ro<const char *, [] { return zero_realloc_mode_names[unsigned(opt.zero_realloc_action)]; }>("zero_realloc"),
    ro<unsigned, [] { return opt.debug_double_free_max_scan; }>("debug_double_free_max_scan"),
    ro<bool, [] { return opt.disable_large_size_classes; }>("disable_large_size_classes"),
    ro<size_t, [] { return opt.process_madvise_max_batch; }>("process_madvise_max_batch"),
    named("malloc_conf", opt_malloc_conf_node),
};

/// --- tcache, arena, arenas -------------------------------------------------------------------------------------------

constexpr CtlNode tcache_node[] = {
    leaf("create", ctl::tcacheCreate),
    leaf("flush", ctl::tcacheFlush),
    leaf("destroy", ctl::tcacheDestroy),
};

constexpr CtlNode arena_i_node[] = {
    leaf("initialized", ctl::arenaIInitialized),
    leaf("decay", ctl::arenaIDecay),
    leaf("purge", ctl::arenaIPurge),
    leaf("reset", ctl::arenaIReset),
    leaf("destroy", ctl::arenaIDestroy),
    leaf("dss", ctl::arenaIDss),
    /// Undocumented for now, since we anticipate an arena API in flux after we cut the last 5-series release.
    leaf("oversize_threshold", ctl::arenaIOversizeThreshold),
    leaf("dirty_decay_ms", ctl::arenaIDirtyDecayMs),
    leaf("muzzy_decay_ms", ctl::arenaIMuzzyDecayMs),
    leaf("extent_hooks", ctl::arenaIExtentHooks),
    leaf("retain_grow_limit", ctl::arenaIRetainGrowLimit),
    leaf("name", ctl::arenaIName),
};
constexpr CtlNode super_arena_i_node[] = {super(arena_i_node)};

constexpr CtlNode arenas_bin_i_node[] = {
    ro<size_t, [](const size_t * mib) { return binInfoOfMib(mib).reg_size; }>("size"),
    ro<uint32_t, [](const size_t * mib) { return binInfoOfMib(mib).nregs; }>("nregs"),
    ro<size_t, [](const size_t * mib) { return binInfoOfMib(mib).slab_size; }>("slab_size"),
    ro<uint32_t, [](const size_t * mib) { return binInfoOfMib(mib).n_shards; }>("nshards"),
};
constexpr CtlNode super_arenas_bin_i_node[] = {super(arenas_bin_i_node)};

constexpr CtlNode arenas_lextent_i_node[] = {
    ro<size_t, [](const size_t * mib) { return lextentSizeOfMib(mib); }>("size"),
};
constexpr CtlNode super_arenas_lextent_i_node[] = {super(arenas_lextent_i_node)};

constexpr CtlNode arenas_node[] = {
    leaf("narenas", ctl::arenasNarenas),
    leaf("dirty_decay_ms", ctl::arenasDirtyDecayMs),
    leaf("muzzy_decay_ms", ctl::arenasMuzzyDecayMs),
    ro<size_t, [] { return QUANTUM; }>("quantum"),
    ro<size_t, [] { return PAGE; }>("page"),
    ro<size_t, [] { return HUGEPAGE; }>("hugepage"),
    leaf("tcache_max", ctl::arenasTcacheMax),
    ro<unsigned, [] { return SC_NBINS; }>("nbins"),
    leaf("nhbins", ctl::arenasNhbins),
    indexed("bin", ctl::arenasBinIIndex, super_arenas_bin_i_node),
    ro<unsigned, [] { return SC_NSIZES - SC_NBINS; }>("nlextents"),
    indexed("lextent", ctl::arenasLextentIIndex, super_arenas_lextent_i_node),
    leaf("create", ctl::arenasCreate),
    leaf("lookup", ctl::arenasLookup),
};

/// --- prof ------------------------------------------------------------------------------------------------------------

constexpr CtlNode prof_stats_bins_i_node[] = {
    leaf("live", ctl::profStatsBinsILive),
    leaf("accum", ctl::profStatsBinsIAccum),
};
constexpr CtlNode super_prof_stats_bins_i_node[] = {super(prof_stats_bins_i_node)};

constexpr CtlNode prof_stats_lextents_i_node[] = {
    leaf("live", ctl::profStatsLextentsILive),
    leaf("accum", ctl::profStatsLextentsIAccum),
};
constexpr CtlNode super_prof_stats_lextents_i_node[] = {super(prof_stats_lextents_i_node)};

constexpr CtlNode prof_stats_node[] = {
    indexed("bins", ctl::profStatsBinsIIndex, super_prof_stats_bins_i_node),
    indexed("lextents", ctl::profStatsLextentsIIndex, super_prof_stats_lextents_i_node),
};

constexpr CtlNode prof_node[] = {
    leaf("thread_active_init", ctl::profThreadActiveInit),
    leaf("active", ctl::profActive),
    leaf("dump", ctl::profDump),
    leaf("gdump", ctl::profGdump),
    leaf("prefix", ctl::profPrefix),
    leaf("reset", ctl::profReset),
    leaf("interval", ctl::profInterval),
    leaf("lg_sample", ctl::profLgSample),
    leaf("log_start", ctl::profLogStart),
    leaf("log_stop", ctl::profLogStop),
    named("stats", prof_stats_node),
};

/// --- stats.arenas.<i> ------------------------------------------------------------------------------------------------

constexpr CtlNode stats_arenas_i_small_node[] = {
    leaf("allocated", ctl::statsArenasISmallAllocated),
    leaf("nmalloc", ctl::statsArenasISmallNmalloc),
    leaf("ndalloc", ctl::statsArenasISmallNdalloc),
    leaf("nrequests", ctl::statsArenasISmallNrequests),
    leaf("nfills", ctl::statsArenasISmallNfills),
    leaf("nflushes", ctl::statsArenasISmallNflushes),
};

constexpr CtlNode stats_arenas_i_large_node[] = {
    leaf("allocated", ctl::statsArenasILargeAllocated),
    leaf("nmalloc", ctl::statsArenasILargeNmalloc),
    leaf("ndalloc", ctl::statsArenasILargeNdalloc),
    leaf("nrequests", ctl::statsArenasILargeNrequests),
    leaf("nfills", ctl::statsArenasILargeNfills),
    leaf("nflushes", ctl::statsArenasILargeNflushes),
};

constexpr CtlNode stats_arenas_i_bins_j_node[] = {
    leaf("nmalloc", ctl::statsArenasIBinsJNmalloc),
    leaf("ndalloc", ctl::statsArenasIBinsJNdalloc),
    leaf("nrequests", ctl::statsArenasIBinsJNrequests),
    leaf("curregs", ctl::statsArenasIBinsJCurregs),
    leaf("nfills", ctl::statsArenasIBinsJNfills),
    leaf("nflushes", ctl::statsArenasIBinsJNflushes),
    leaf("nslabs", ctl::statsArenasIBinsJNslabs),
    leaf("nreslabs", ctl::statsArenasIBinsJNreslabs),
    leaf("curslabs", ctl::statsArenasIBinsJCurslabs),
    leaf("nonfull_slabs", ctl::statsArenasIBinsJNonfullSlabs),
    named("mutex", mutex_prof_node<&ctl::binMutexProfData>),
};
constexpr CtlNode super_stats_arenas_i_bins_j_node[] = {super(stats_arenas_i_bins_j_node)};

constexpr CtlNode stats_arenas_i_lextents_j_node[] = {
    leaf("nmalloc", ctl::statsArenasILextentsJNmalloc),
    leaf("ndalloc", ctl::statsArenasILextentsJNdalloc),
    leaf("nrequests", ctl::statsArenasILextentsJNrequests),
    leaf("curlextents", ctl::statsArenasILextentsJCurlextents),
};
constexpr CtlNode super_stats_arenas_i_lextents_j_node[] = {super(stats_arenas_i_lextents_j_node)};

constexpr CtlNode stats_arenas_i_extents_j_node[] = {
    leaf("ndirty", ctl::statsArenasIExtentsJNdirty),
    leaf("nmuzzy", ctl::statsArenasIExtentsJNmuzzy),
    leaf("nretained", ctl::statsArenasIExtentsJNretained),
    leaf("dirty_bytes", ctl::statsArenasIExtentsJDirtyBytes),
    leaf("muzzy_bytes", ctl::statsArenasIExtentsJMuzzyBytes),
    leaf("retained_bytes", ctl::statsArenasIExtentsJRetainedBytes),
};
constexpr CtlNode super_stats_arenas_i_extents_j_node[] = {super(stats_arenas_i_extents_j_node)};

/// jemalloc: MUTEX_PROF_ARENA_MUTEXES
constexpr CtlNode stats_arenas_i_mutexes_node[] = {
    named("large", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_large>>),
    named("extent_avail", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_extent_avail>>),
    named("extents_dirty", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_extents_dirty>>),
    named("extents_muzzy", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_extents_muzzy>>),
    named("extents_retained", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_extents_retained>>),
    named("decay_dirty", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_decay_dirty>>),
    named("decay_muzzy", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_decay_muzzy>>),
    named("base", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_base>>),
    named("tcache_list", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_tcache_list>>),
    named("hpa_shard", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_hpa_shard>>),
    named("hpa_shard_grow", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_hpa_shard_grow>>),
    named("hpa_sec", mutex_prof_node<&ctl::arenaMutexProfDataOf<arena_prof_mutex_hpa_sec>>),
};

/// HPA is dropped: all its statistics are zero (in jemalloc too, since HPA is never enabled in ClickHouse). The
/// same children are used for `slabs`, `full_slabs`, `empty_slabs` and `nonfull_slabs.<j>`.
constexpr CtlNode stats_arenas_i_hpa_shard_slabs_node[] = {
    zeroStat<size_t>("npageslabs_nonhuge"),
    zeroStat<size_t>("npageslabs_huge"),
    zeroStat<size_t>("nactive_nonhuge"),
    zeroStat<size_t>("nactive_huge"),
    zeroStat<size_t>("ndirty_nonhuge"),
    zeroStat<size_t>("ndirty_huge"),
};
constexpr CtlNode super_stats_arenas_i_hpa_shard_nonfull_slabs_j_node[] = {super(stats_arenas_i_hpa_shard_slabs_node)};

constexpr CtlNode stats_arenas_i_hpa_shard_node[] = {
    zeroStat<size_t>("npageslabs"),
    zeroStat<size_t>("nactive"),
    zeroStat<size_t>("ndirty"),

    named("slabs", stats_arenas_i_hpa_shard_slabs_node),

    zeroStat<uint64_t>("npurge_passes"),
    zeroStat<uint64_t>("npurges"),
    zeroStat<uint64_t>("nhugifies"),
    zeroStat<uint64_t>("nhugify_failures"),
    zeroStat<uint64_t>("ndehugifies"),

    named("full_slabs", stats_arenas_i_hpa_shard_slabs_node),
    named("empty_slabs", stats_arenas_i_hpa_shard_slabs_node),
    indexed("nonfull_slabs", ctl::statsArenasIHpaShardNonfullSlabsJIndex, super_stats_arenas_i_hpa_shard_nonfull_slabs_j_node),
};

constexpr CtlNode stats_arenas_i_node[] = {
    leaf("nthreads", ctl::statsArenasINthreads),
    leaf("uptime", ctl::statsArenasIUptime),
    leaf("dss", ctl::statsArenasIDss),
    leaf("dirty_decay_ms", ctl::statsArenasIDirtyDecayMs),
    leaf("muzzy_decay_ms", ctl::statsArenasIMuzzyDecayMs),
    leaf("pactive", ctl::statsArenasIPactive),
    leaf("pdirty", ctl::statsArenasIPdirty),
    leaf("pmuzzy", ctl::statsArenasIPmuzzy),
    leaf("mapped", ctl::statsArenasIMapped),
    leaf("retained", ctl::statsArenasIRetained),
    leaf("extent_avail", ctl::statsArenasIExtentAvail),
    leaf("dirty_npurge", ctl::statsArenasIDirtyNpurge),
    leaf("dirty_nmadvise", ctl::statsArenasIDirtyNmadvise),
    leaf("dirty_purged", ctl::statsArenasIDirtyPurged),
    leaf("muzzy_npurge", ctl::statsArenasIMuzzyNpurge),
    leaf("muzzy_nmadvise", ctl::statsArenasIMuzzyNmadvise),
    leaf("muzzy_purged", ctl::statsArenasIMuzzyPurged),
    leaf("base", ctl::statsArenasIBase),
    leaf("internal", ctl::statsArenasIInternal),
    leaf("metadata_edata", ctl::statsArenasIMetadataEdata),
    leaf("metadata_rtree", ctl::statsArenasIMetadataRtree),
    leaf("metadata_thp", ctl::statsArenasIMetadataThp),
    leaf("tcache_bytes", ctl::statsArenasITcacheBytes),
    leaf("tcache_stashed_bytes", ctl::statsArenasITcacheStashedBytes),
    leaf("resident", ctl::statsArenasIResident),
    leaf("abandoned_vm", ctl::statsArenasIAbandonedVm),
    /// SEC is dropped: zero (in jemalloc too). Note the order: `noflush` before `flush`.
    zeroStat<size_t>("hpa_sec_bytes"),
    zeroStat<size_t>("hpa_sec_hits"),
    zeroStat<size_t>("hpa_sec_misses"),
    zeroStat<size_t>("hpa_sec_dalloc_noflush"),
    zeroStat<size_t>("hpa_sec_dalloc_flush"),
    zeroStat<size_t>("hpa_sec_overfills"),
    named("small", stats_arenas_i_small_node),
    named("large", stats_arenas_i_large_node),
    indexed("bins", ctl::statsArenasIBinsJIndex, super_stats_arenas_i_bins_j_node),
    indexed("lextents", ctl::statsArenasILextentsJIndex, super_stats_arenas_i_lextents_j_node),
    indexed("extents", ctl::statsArenasIExtentsJIndex, super_stats_arenas_i_extents_j_node),
    named("mutexes", stats_arenas_i_mutexes_node),
    named("hpa_shard", stats_arenas_i_hpa_shard_node),
};
constexpr CtlNode super_stats_arenas_i_node[] = {super(stats_arenas_i_node)};

/// --- stats -----------------------------------------------------------------------------------------------------------

constexpr CtlNode stats_background_thread_node[] = {
    leaf("num_threads", ctl::statsBackgroundThreadNumThreads),
    leaf("num_runs", ctl::statsBackgroundThreadNumRuns),
    leaf("run_interval", ctl::statsBackgroundThreadRunInterval),
};

/// jemalloc: MUTEX_PROF_GLOBAL_MUTEXES
constexpr CtlNode stats_mutexes_node[] = {
    named("background_thread", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_background_thread>>),
    named("max_per_bg_thd", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_max_per_bg_thd>>),
    named("ctl", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_ctl>>),
    named("prof", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof>>),
    named("prof_thds_data", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof_thds_data>>),
    named("prof_dump", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof_dump>>),
    named("prof_recent_alloc", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof_recent_alloc>>),
    named("prof_recent_dump", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof_recent_dump>>),
    named("prof_stats", mutex_prof_node<&ctl::globalMutexProfData<global_prof_mutex_prof_stats>>),
    leaf("reset", ctl::statsMutexesReset),
};

constexpr CtlNode approximate_stats_node[] = {
    leaf("active", ctl::approximateStatsActive),
};

constexpr CtlNode stats_node[] = {
    leaf("allocated", ctl::statsAllocated),
    leaf("active", ctl::statsActive),
    leaf("metadata", ctl::statsMetadata),
    leaf("metadata_edata", ctl::statsMetadataEdata),
    leaf("metadata_rtree", ctl::statsMetadataRtree),
    leaf("metadata_thp", ctl::statsMetadataThp),
    leaf("resident", ctl::statsResident),
    leaf("mapped", ctl::statsMapped),
    leaf("retained", ctl::statsRetained),
    named("background_thread", stats_background_thread_node),
    named("mutexes", stats_mutexes_node),
    indexed("arenas", ctl::statsArenasIIndex, super_stats_arenas_i_node),
    leaf("zero_reallocs", ctl::statsZeroReallocs),
};

/// --- experimental ----------------------------------------------------------------------------------------------------

constexpr CtlNode experimental_hooks_node[] = {
    leaf("install", ctl::experimentalHooksInstall),
    leaf("remove", ctl::experimentalHooksRemove),
    leaf("prof_backtrace", ctl::experimentalHooksProfBacktrace),
    leaf("prof_dump", ctl::experimentalHooksProfDump),
    leaf("prof_sample", ctl::experimentalHooksProfSample),
    leaf("prof_sample_free", ctl::experimentalHooksProfSampleFree),
    leaf("safety_check_abort", ctl::experimentalHooksSafetyCheckAbort),
    leaf("thread_event", ctl::experimentalHooksThreadEvent),
};

constexpr CtlNode experimental_thread_node[] = {
    leaf("activity_callback", ctl::experimentalThreadActivityCallback),
};

constexpr CtlNode experimental_utilization_node[] = {
    leaf("query", ctl::experimentalUtilizationQuery),
    leaf("batch_query", ctl::experimentalUtilizationBatchQuery),
};

constexpr CtlNode experimental_arenas_i_node[] = {
    leaf("pactivep", ctl::experimentalArenasIPactivep),
};
constexpr CtlNode super_experimental_arenas_i_node[] = {super(experimental_arenas_i_node)};

constexpr CtlNode experimental_prof_recent_node[] = {
    leaf("alloc_max", ctl::experimentalProfRecentAllocMax),
    leaf("alloc_dump", ctl::experimentalProfRecentAllocDump),
};

constexpr CtlNode experimental_node[] = {
    named("hooks", experimental_hooks_node),
    named("utilization", experimental_utilization_node),
    indexed("arenas", ctl::experimentalArenasIIndex, super_experimental_arenas_i_node),
    leaf("arenas_create_ext", ctl::experimentalArenasCreateExt),
    named("prof_recent", experimental_prof_recent_node),
    leaf("batch_alloc", ctl::experimentalBatchAlloc),
    named("thread", experimental_thread_node),
};

/// --- root ------------------------------------------------------------------------------------------------------------

constexpr CtlNode root_node[] = {
    leaf("version", ctl::version),
    leaf("epoch", ctl::epoch),
    leaf("background_thread", ctl::backgroundThread),
    leaf("max_background_threads", ctl::maxBackgroundThreads),
    named("thread", thread_node),
    named("config", config_node),
    named("opt", opt_node),
    named("tcache", tcache_node),
    indexed("arena", ctl::arenaIIndex, super_arena_i_node),
    named("arenas", arenas_node),
    named("prof", prof_node),
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
/// `CTL_MAX_DEPTH`.
constexpr bool checkTree(const CtlNode & node, size_t depth)
{
    if (depth > CTL_MAX_DEPTH)
        return false;
    if (node.isLeaf())
        return node.children == nullptr && node.nchildren == 0 && node.index == nullptr;
    if (node.children == nullptr || node.nchildren == 0)
        return false;
    if (node.isIndexed())
    {
        const CtlNode & super_node = node.children[0];
        if (node.nchildren != 1 || super_node.name[0] != '\0' || super_node.isLeaf() || super_node.isIndexed())
            return false;
        for (size_t i = 0; i < super_node.nchildren; ++i)
            if (!checkTree(super_node.children[i], depth + 2))
                return false;
        return true;
    }
    for (size_t i = 0; i < node.nchildren; ++i)
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

constexpr CtlNode root_check_node = super(root_node);
static_assert(checkTree(root_check_node, 0));

}

constinit const CtlNode ctl_super_root_node[1] = {super(root_node)};

}
