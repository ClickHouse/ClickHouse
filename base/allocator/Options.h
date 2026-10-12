#pragma once

/// All run-time options of the allocator (jemalloc's `opt_*` globals) in one constant-initialized struct `options`,
/// the option enums and their name tables.
///
/// jemalloc defines the options in the files of the subsystems that use them (`jemalloc.c`, `arena.c`, `tcache.c`,
/// `prof.c`, `pages.c`, ...). Here they are grouped by subsystem inside `struct Options`; the comment of every field
/// names the original variable. The defaults are jemalloc's compiled defaults for the platform (before the compiled-in
/// `malloc_conf` string is applied by `mallocConfInit`, see MallocConf.h).
///
/// Values are set by the configuration parser during initialization (`mallocConfInit`) and by later boot steps
/// (e.g. `num_arenas`, `per_cpu_arena`, `max_background_threads`, `transparent_huge_pages`, `huge_page_allocator`); after initialization
/// they are read-only
/// and reported by the `opt.*` mallctls.
///
/// Options of dropped features (HPA, SEC, DSS allocation, `profiling_log`) are parsed and stored, so that `opt.*` and the
/// stats output report the same values as jemalloc, but have no effect.
///
/// jemalloc's `config_debug` is never enabled in ClickHouse, so the defaults are those of a non-debug build even when
/// `ALLOCATOR_DEBUG` enables our internal assertions (e.g. `opt.abort` is false, `opt.junk` is "false").

#include <allocator/Common.h>
#include <allocator/FixedPoint.h>
#include <allocator/Nanoseconds.h>
#include <allocator/SizeClassConstants.h>

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <sys/types.h>

namespace jemalloc
{

/// --- Enums and name tables ---------------------------------------------------------------------------------------

/// What `realloc(ptr, 0)` does (the `zero_realloc` option).
/// jemalloc: zero_realloc_action_t
enum class ZeroReallocAction : unsigned
{
    /// `realloc(ptr, 0)` is `free(ptr); return malloc(0);`. jemalloc: zero_realloc_action_alloc
    Alloc = 0,
    /// `realloc(ptr, 0)` is `free(ptr)`. jemalloc: zero_realloc_action_free
    Free = 1,
    /// `realloc(ptr, 0)` aborts. jemalloc: zero_realloc_action_abort
    Abort = 2,
};

/// jemalloc: zero_realloc_mode_names
extern const char * const zero_realloc_mode_names[3];

/// The `percpu_arena` option. The parser stores one of the first three values; `malloc_init_narenas` adds
/// `EnabledBase` to the enabled modes (so `opt.percpu_arena` indexes `per_cpu_arena_mode_names` either way).
/// jemalloc: percpu_arena_mode_t
enum class PerCPUArenaMode : unsigned
{
    /// jemalloc: percpu_arena_uninit
    PerCPUUninitialized = 0,
    /// jemalloc: per_phycpu_arena_uninit
    PerPhysicalCPUUninitialized = 1,
    /// All non-disabled modes must come after `Disabled`. jemalloc: percpu_arena_disabled
    Disabled = 2,
    /// jemalloc: percpu_arena = percpu_arena_mode_enabled_base
    PerCPU = 3,
    /// Hyper threads share an arena. jemalloc: per_phycpu_arena
    PerPhysicalCPU = 4,
};

/// jemalloc: percpu_arena_mode_names_base
inline constexpr unsigned per_cpu_arena_mode_names_base = 0;
/// Used for option processing. jemalloc: percpu_arena_mode_names_limit
inline constexpr unsigned per_cpu_arena_mode_names_limit = 3;
/// jemalloc: percpu_arena_mode_enabled_base
inline constexpr unsigned per_cpu_arena_mode_enabled_base = 3;

/// jemalloc: PERCPU_ARENA_ENABLED
constexpr bool perCPUArenaEnabled(PerCPUArenaMode mode)
{
    return unsigned(mode) >= per_cpu_arena_mode_enabled_base;
}

/// jemalloc: percpu_arena_mode_names = {"percpu", "phycpu", "disabled", "percpu", "phycpu"}
extern const char * const per_cpu_arena_mode_names[5];

/// The `metadata_thp` option (`MetadataTransparentHugePagesMode`, `metadata_transparent_huge_pages_mode_limit`,
/// `metadata_transparent_huge_pages_mode_names` are defined in
/// Pages.h), the `thp` option (`TransparentHugePagesMode`, `transparent_huge_pages_mode_names_limit`, `transparent_huge_pages_mode_names`
/// in Pages.h) and the `dss` option
/// (`SbrkPrecedence`, `SBRK_DEFAULT`, `sbrk_precedence_names` in ExtentHooks.h). Declared opaquely here to keep this header light.
enum class MetadataTransparentHugePagesMode : unsigned;
enum class TransparentHugePagesMode : unsigned;
enum class SbrkPrecedence : unsigned;

/// The current default DSS precedence for new arenas (`dss` option, `arena.<MALLCTL_ARENAS_ALL>.dss`).
/// `Disabled` when the platform has no DSS (Darwin).
/// jemalloc: extent_dss_prec_get
SbrkPrecedence extentSbrkPrecedenceGet();

/// Returns true on error (a non-`Disabled` precedence on a platform without DSS).
/// jemalloc: extent_dss_prec_set
bool extentSbrkPrecedenceSet(SbrkPrecedence sbrk_precedence);

/// jemalloc: hpa_hugify_style_t (HPA is dropped; the option is only stored and reported).
enum class HugePageAllocatorHugifyStyle : unsigned
{
    /// jemalloc: hpa_hugify_style_auto
    Auto = 0,
    /// jemalloc: hpa_hugify_style_none
    None = 1,
    /// jemalloc: hpa_hugify_style_eager
    Eager = 2,
    /// jemalloc: hpa_hugify_style_lazy
    Lazy = 3,
};

/// jemalloc: hpa_hugify_style_limit
inline constexpr unsigned huge_page_allocator_hugify_style_limit = 4;

/// jemalloc: hpa_hugify_style_names = {"auto", "none", "eager", "lazy"}
extern const char * const huge_page_allocator_hugify_style_names[4];

/// jemalloc: prof_time_res_mode_names = {"default", "high"} (`ProfTimeRes` is defined in NsTime.h)
extern const char * const profiling_time_resolution_mode_names[2];

/// The values of the `junk` option as reported by `opt.junk`.
inline constexpr const char * JUNK_TRUE = "true";
inline constexpr const char * JUNK_FALSE = "false";
inline constexpr const char * JUNK_ALLOC = "alloc";
inline constexpr const char * JUNK_FREE = "free";

/// --- Constants that bound option values --------------------------------------------------------------------------

/// The characters accepted by `stats_print_options` / `stats_interval_options`, in the order of jemalloc's
/// `STATS_PRINT_OPTIONS` (`stats.h`): json, general, merged, destroyed, unmerged, bins, large, mutex, extents, hpa.
inline constexpr char stats_print_option_chars[] = "Jgmdablxeh";
/// jemalloc: stats_print_tot_num_options
inline constexpr size_t stats_print_total_num_options = sizeof(stats_print_option_chars) - 1;

/// jemalloc: TCACHE_LG_MAXCLASS_LIMIT, TCACHE_MAXCLASS_LIMIT, TCACHE_NBINS_MAX (`tcache_types.h`)
inline constexpr unsigned THREAD_CACHE_LOG2_MAX_CLASS_LIMIT = LOG2_USABLE_SIZE_GROW_SLOW_THRESHOLD;
inline constexpr size_t THREAD_CACHE_MAX_CLASS_LIMIT = size_t(1) << THREAD_CACHE_LOG2_MAX_CLASS_LIMIT;
inline constexpr unsigned THREAD_CACHE_NUM_BINS_MAX
    = SIZE_CLASS_NUM_BINS + unsigned(SIZE_CLASS_GROUP_SIZE) * (THREAD_CACHE_LOG2_MAX_CLASS_LIMIT - SIZE_CLASS_LOG2_LARGE_MIN_CLASS) + 1;

/// jemalloc: MAX_BACKGROUND_THREAD_LIMIT, DEFAULT_NUM_BACKGROUND_THREAD (`background_thread_structs.h`)
inline constexpr size_t MAX_BACKGROUND_THREAD_LIMIT = MALLOCX_ARENA_LIMIT;
inline constexpr size_t DEFAULT_NUM_BACKGROUND_THREAD = 4;

/// jemalloc: PROF_BT_MAX_LIMIT (not `JEMALLOC_PROF_GCC`), PROF_DUMP_FILENAME_LEN (`prof_types.h`)
inline constexpr unsigned PROFILING_BACKTRACE_MAX_LIMIT = UINT_MAX;
inline constexpr size_t PROFILING_DUMP_FILENAME_LEN = PATH_MAX + 1;

/// jemalloc: PROCESS_MADVISE_MAX_BATCH_LIMIT (`JEMALLOC_HAVE_PROCESS_MADVISE` is not defined on any platform).
inline constexpr size_t PROCESS_MADVISE_MAX_BATCH_LIMIT = 0;

/// jemalloc: SEC_OPTS_* (`sec_opts.h`)
inline constexpr size_t SMALL_EXTENT_CACHE_NUM_SHARDS_DEFAULT = 2;
inline constexpr size_t SMALL_EXTENT_CACHE_BATCH_FILL_EXTRA_DEFAULT = 3;
inline constexpr size_t SMALL_EXTENT_CACHE_MAX_ALLOC_DEFAULT = (32 * 1024) < PAGE ? PAGE : (32 * 1024);
inline constexpr size_t SMALL_EXTENT_CACHE_MAX_BYTES_DEFAULT
    = (256 * 1024) < (4 * SMALL_EXTENT_CACHE_MAX_ALLOC_DEFAULT) ? (4 * SMALL_EXTENT_CACHE_MAX_ALLOC_DEFAULT) : (256 * 1024);

/// jemalloc: HUGEPAGE_PAGES (`pages.h`)
inline constexpr size_t HUGE_PAGE_PAGES = HUGE_PAGE / PAGE;

/// --- Option groups -----------------------------------------------------------------------------------------------

/// HPA options (dropped feature: stored and reported only).
/// jemalloc: hpa_shard_opts_t, HPA_SHARD_OPTS_DEFAULT (`hpa_opts.h`)
struct HugePageShardOptions
{
    size_t slab_max_alloc = 64 * 1024;
    size_t hugification_threshold = HUGE_PAGE * 95 / 100;
    FixedPoint dirty_multiplier = fixed_point::initPercent(25);
    bool deferral_allowed = false;
    uint64_t hugify_delay_ms = 10 * 1000;
    bool hugify_sync = false;
    uint64_t min_purge_interval_ms = 5 * 1000;
    ssize_t experimental_max_purge_num_huge_pages = -1;
    size_t purge_threshold = PAGE;
    uint64_t min_purge_delay_ms = 0;
    HugePageAllocatorHugifyStyle hugify_style = HugePageAllocatorHugifyStyle::Lazy;
};

/// SEC options (dropped feature: stored and reported only).
/// jemalloc: sec_opts_t, SEC_OPTS_DEFAULT (`sec_opts.h`)
struct SmallExtentCacheOptions
{
    size_t num_shards = SMALL_EXTENT_CACHE_NUM_SHARDS_DEFAULT;
    size_t max_alloc = SMALL_EXTENT_CACHE_MAX_ALLOC_DEFAULT;
    size_t max_bytes = SMALL_EXTENT_CACHE_MAX_BYTES_DEFAULT;
    size_t batch_fill_extra = SMALL_EXTENT_CACHE_BATCH_FILL_EXTRA_DEFAULT;
};

struct Options
{
    /// --- jemalloc.c ---

    /// The `/etc/malloc.configuration` symlink target (never read in ClickHouse's configuration, so always null).
    /// jemalloc: opt_malloc_conf_symlink
    const char * malloc_conf_symlink = nullptr;
    /// The value of `MALLOC_CONF` at initialization, if set. jemalloc: opt_malloc_conf_env_var
    const char * malloc_conf_env_variable = nullptr;

    /// jemalloc: opt_abort (true only with `JEMALLOC_DEBUG`)
    bool abort = false;
    /// jemalloc: opt_abort_conf (true only with `JEMALLOC_DEBUG`)
    bool abort_configuration = false;
    /// Intentionally default off, even with debug builds. jemalloc: opt_confirm_conf
    bool confirm_configuration = false;
    /// One of `JUNK_TRUE`, `JUNK_FALSE`, `JUNK_ALLOC`, `JUNK_FREE`. jemalloc: opt_junk
    const char * junk = JUNK_FALSE;
    /// jemalloc: opt_junk_alloc
    bool junk_alloc = false;
    /// jemalloc: opt_junk_free
    bool junk_free = false;
    /// False where `MADV_DONTNEED` is expected to zero memory (`JEMALLOC_PURGE_MADVISE_DONTNEED_ZEROS`, Linux).
    /// jemalloc: opt_trust_madvise
    bool trust_madvise = !config::purge_madvise_dontneed_zeros;
    /// jemalloc: opt_cache_oblivious
    bool cache_oblivious = config::cache_oblivious;
    /// jemalloc: opt_zero_realloc_action
    ZeroReallocAction zero_realloc_action = config::zero_realloc_default_free ? ZeroReallocAction::Free : ZeroReallocAction::Alloc;
    /// Disabling large size classes is the default behavior; configurable mainly for debugging.
    /// jemalloc: opt_disable_large_size_classes
    bool disable_large_size_classes = true;
    /// Never settable in ClickHouse's configuration (`config_utrace`, `config_abort_on_out_of_memory`, `config_enable_cxx` are
    /// false; s390x and FreeBSD ppc64le have `JEMALLOC_ENABLE_CXX`, but only `experimental_infallible_new` depends on it).
    /// jemalloc: opt_utrace, opt_xmalloc, opt_experimental_infallible_new
    bool utrace = false;
    bool abort_on_out_of_memory = false;
    bool experimental_infallible_new = false;
    /// jemalloc: opt_experimental_tcache_gc
    bool experimental_thread_cache_gc = true;
    /// jemalloc: opt_zero
    bool zero = false;
    /// 0 means "computed at boot" (`malloc_init_narenas` replaces it). jemalloc: opt_narenas
    unsigned num_arenas = 0;
    /// jemalloc: opt_narenas_ratio
    FixedPoint num_arenas_ratio = fixed_point::initInt(4);
    /// Forced to 0 after parsing (jemalloc's `config_debug` is false). jemalloc: opt_debug_double_free_max_scan
    unsigned debug_double_free_max_scan = 32; /// SAFETY_CHECK_DOUBLE_FREE_MAX_SCAN_DEFAULT
    /// jemalloc: opt_calloc_madvise_threshold (CALLOC_MADVISE_THRESHOLD_DEFAULT)
    size_t calloc_madvise_threshold = size_t(1) << 23;

    /// --- HPA / SEC (dropped: parsed, stored, reported; `huge_page_allocator` is reset to false at boot, see
    /// `hugePageAllocatorDisableUnsupported`) ---

    /// jemalloc: opt_hpa
    bool huge_page_allocator = false;
    /// jemalloc: opt_hpa_opts
    HugePageShardOptions huge_page_allocator_options;
    /// jemalloc: opt_hpa_sec_opts
    SmallExtentCacheOptions small_extent_cache_options;
    /// jemalloc: opt_experimental_hpa_start_huge_if_thp_always (`hpa.c`)
    bool experimental_huge_page_allocator_start_huge = true;
    /// jemalloc: opt_experimental_hpa_enforce_hugify (`hpa.c`)
    bool experimental_huge_page_allocator_enforce_hugify = false;

    /// --- arena.c ---

    /// jemalloc: opt_percpu_arena (PERCPU_ARENA_DEFAULT)
    PerCPUArenaMode per_cpu_arena = PerCPUArenaMode::Disabled;
    /// jemalloc: opt_dirty_decay_ms (DIRTY_DECAY_MS_DEFAULT)
    ssize_t dirty_decay_ms = 10 * 1000;
    /// jemalloc: opt_muzzy_decay_ms (MUZZY_DECAY_MS_DEFAULT)
    ssize_t muzzy_decay_ms = 0;
    /// Allocations of at least this size use the dedicated huge arena; 0 disables. jemalloc: opt_oversize_threshold
    size_t oversize_threshold = size_t(8) << 20; /// OVERSIZE_THRESHOLD_DEFAULT
    /// jemalloc: opt_huge_arena_pac_thp
    bool huge_arena_transparent_huge_pages = false;

    /// --- base.c, pages.c, extent_mmap.c, extent_dss.c, extent.c ---

    /// jemalloc: opt_metadata_thp (METADATA_THP_DEFAULT)
    MetadataTransparentHugePagesMode metadata_transparent_huge_pages
        = MetadataTransparentHugePagesMode(0); /// MetadataTransparentHugePagesMode::Disabled
    /// Set to `NotSupported` by the pages boot when THP is unavailable. jemalloc: opt_thp
    TransparentHugePagesMode transparent_huge_pages
        = TransparentHugePagesMode(0); /// TRANSPARENT_HUGE_PAGES_MODE_DEFAULT = TransparentHugePagesMode::DoNothing
    /// jemalloc: opt_retain (`JEMALLOC_RETAIN`)
    bool retain = config::retain;
    /// One of `sbrk_precedence_names`. jemalloc: opt_dss
    const char * sbrk = "secondary"; /// SBRK_DEFAULT
    /// jemalloc: opt_lg_extent_max_active_fit (LG_EXTENT_MAX_ACTIVE_FIT_DEFAULT)
    size_t log2_extent_max_active_fit = 6;
    /// jemalloc: opt_process_madvise_max_batch (0 without `JEMALLOC_HAVE_PROCESS_MADVISE`)
    size_t process_madvise_max_batch = 0;

    /// --- mutex.c ---

    /// Spin iterations before blocking; -1 spins forever. jemalloc: opt_mutex_max_spin
    int64_t mutex_max_spin = 600;

    /// --- stats.c ---

    /// jemalloc: opt_stats_print
    bool stats_print = false;
    /// jemalloc: opt_stats_print_opts
    char stats_print_options[stats_print_total_num_options + 1] = "";
    /// jemalloc: opt_stats_interval (STATS_INTERVAL_DEFAULT)
    int64_t stats_interval = -1;
    /// jemalloc: opt_stats_interval_opts
    char stats_interval_options[stats_print_total_num_options + 1] = "";

    /// --- tcache.c ---

    /// jemalloc: opt_tcache
    bool thread_cache = true;
    /// jemalloc: opt_tcache_max
    size_t thread_cache_max = size_t(1) << 15;
    /// jemalloc: opt_tcache_nslots_small_min, opt_tcache_nslots_small_max, opt_tcache_nslots_large
    unsigned thread_cache_num_slots_small_min = 20;
    unsigned thread_cache_num_slots_small_max = 200;
    unsigned thread_cache_num_slots_large = 20;
    /// jemalloc: opt_lg_tcache_nslots_mul
    ssize_t log2_thread_cache_num_slots_multiplier = 1;
    /// jemalloc: opt_tcache_gc_incr_bytes
    size_t thread_cache_gc_increment_bytes = 65536;
    /// jemalloc: opt_tcache_gc_delay_bytes
    size_t thread_cache_gc_delay_bytes = 0;
    /// jemalloc: opt_lg_tcache_flush_small_div, opt_lg_tcache_flush_large_div
    unsigned log2_thread_cache_flush_small_division = 1;
    unsigned log2_thread_cache_flush_large_division = 1;
    /// The per-bin `num_cached_max` set by `thread_cache_num_cached_max` (`cache_bin_info_t::num_cached_max` values; the tcache boot
    /// fills in the bins that were not set), and which bins were set.
    /// jemalloc: opt_tcache_ncached_max, opt_tcache_ncached_max_set (static in `tcache.c`)
    uint16_t thread_cache_num_cached_max[THREAD_CACHE_NUM_BINS_MAX] = {};
    bool thread_cache_num_cached_max_set[THREAD_CACHE_NUM_BINS_MAX] = {};

    /// --- background_thread.c ---

    /// jemalloc: opt_background_thread (BACKGROUND_THREAD_DEFAULT)
    bool background_thread = false;
    /// The background thread boot replaces values above `MAX_BACKGROUND_THREAD_LIMIT` with
    /// `DEFAULT_NUM_BACKGROUND_THREAD`. jemalloc: opt_max_background_threads
    size_t max_background_threads = MAX_BACKGROUND_THREAD_LIMIT + 1;

    /// --- prof.c, prof_log.c, prof_recent.c, prof_stats.c, nstime.c ---

    /// jemalloc: opt_prof
    bool profiling = false;
    /// jemalloc: opt_prof_active
    bool profiling_active = true;
    /// jemalloc: opt_prof_thread_active_init
    bool profiling_thread_active_init = true;
    /// jemalloc: opt_prof_bt_max (PROF_BT_MAX_DEFAULT)
    unsigned profiling_backtrace_max = 128;
    /// jemalloc: opt_lg_prof_sample (LG_PROF_SAMPLE_DEFAULT)
    size_t log2_profiling_sample = 19;
    /// jemalloc: opt_lg_prof_interval (LG_PROF_INTERVAL_DEFAULT)
    ssize_t log2_profiling_interval = -1;
    /// jemalloc: opt_prof_gdump, opt_prof_final, opt_prof_leak, opt_prof_leak_error, opt_prof_accum
    bool profiling_growth_dump = false;
    bool profiling_final = false;
    bool profiling_leak = false;
    bool profiling_leak_error = false;
    bool profiling_accumulated = false;
    /// jemalloc: opt_prof_pid_namespace
    bool profiling_pid_namespace = false;
    /// Initialized with PROF_PREFIX_DEFAULT here (jemalloc does it in `prof_boot0`, before parsing the options).
    /// jemalloc: opt_prof_prefix
    char profiling_prefix[PROFILING_DUMP_FILENAME_LEN] = "jeprof";
    /// jemalloc: opt_prof_sys_thread_name
    bool profiling_system_thread_name = false;
    /// jemalloc: opt_prof_unbias
    bool profiling_unbias = true;
    /// Dropped feature (`profiling_log`): parsed and stored only. Like in jemalloc, there is no `opt.prof_log` mallctl
    /// and the stats output does not print it. jemalloc: opt_prof_log
    bool profiling_log = false;
    /// jemalloc: opt_prof_recent_alloc_max (PROF_RECENT_ALLOC_MAX_DEFAULT)
    ssize_t profiling_recent_alloc_max = 0;
    /// jemalloc: opt_prof_stats
    bool profiling_stats = false;
    /// Which clock `Nanoseconds::profilingUpdate` uses. jemalloc: opt_prof_time_res (`nstime.c`)
    ProfilingTimeResolution profiling_time_resolution = ProfilingTimeResolution::Default;

    /// --- san.c ---

    /// jemalloc: opt_san_guard_large, opt_san_guard_small (SAN_GUARD_*_EVERY_N_EXTENTS_DEFAULT)
    size_t sanitizer_guard_large = 0;
    size_t sanitizer_guard_small = 0;
    /// Only settable with `config::use_after_free_detection`. jemalloc: opt_lg_san_uaf_align (SAN_LG_UAF_ALIGN_DEFAULT)
    ssize_t log2_sanitizer_use_after_free_align = -1;
};

/// jemalloc: all `opt_*` globals.
extern constinit Options options;

}
