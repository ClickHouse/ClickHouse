#pragma once

/// All run-time options of the allocator (jemalloc's `opt_*` globals) in one constant-initialized struct `opt`,
/// the option enums and their name tables.
///
/// jemalloc defines the options in the files of the subsystems that use them (`jemalloc.c`, `arena.c`, `tcache.c`,
/// `prof.c`, `pages.c`, ...). Here they are grouped by subsystem inside `struct Options`; the comment of every field
/// names the original variable. The defaults are jemalloc's compiled defaults for the platform (before the compiled-in
/// `malloc_conf` string is applied by `mallocConfInit`, see Conf.h).
///
/// Values are set by the configuration parser during initialization (`mallocConfInit`) and by later boot steps
/// (e.g. `narenas`, `percpu_arena`, `max_background_threads`, `thp`, `hpa`); after initialization they are read-only
/// and reported by the `opt.*` mallctls.
///
/// Options of dropped features (HPA, SEC, DSS allocation, `prof_log`) are parsed and stored, so that `opt.*` and the
/// stats output report the same values as jemalloc, but have no effect.
///
/// jemalloc's `config_debug` is never enabled in ClickHouse, so the defaults are those of a non-debug build even when
/// `ALLOCATOR_DEBUG` enables our internal assertions (e.g. `opt.abort` is false, `opt.junk` is "false").

#include <allocator/Common.h>
#include <allocator/FixedPoint.h>
#include <allocator/NsTime.h>
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
/// `EnabledBase` to the enabled modes (so `opt.percpu_arena` indexes `percpu_arena_mode_names` either way).
/// jemalloc: percpu_arena_mode_t
enum class PercpuArenaMode : unsigned
{
    /// jemalloc: percpu_arena_uninit
    PercpuUninit = 0,
    /// jemalloc: per_phycpu_arena_uninit
    PerPhycpuUninit = 1,
    /// All non-disabled modes must come after `Disabled`. jemalloc: percpu_arena_disabled
    Disabled = 2,
    /// jemalloc: percpu_arena = percpu_arena_mode_enabled_base
    Percpu = 3,
    /// Hyper threads share an arena. jemalloc: per_phycpu_arena
    PerPhycpu = 4,
};

/// jemalloc: percpu_arena_mode_names_base
inline constexpr unsigned percpu_arena_mode_names_base = 0;
/// Used for option processing. jemalloc: percpu_arena_mode_names_limit
inline constexpr unsigned percpu_arena_mode_names_limit = 3;
/// jemalloc: percpu_arena_mode_enabled_base
inline constexpr unsigned percpu_arena_mode_enabled_base = 3;

/// jemalloc: PERCPU_ARENA_ENABLED
constexpr bool percpuArenaEnabled(PercpuArenaMode mode)
{
    return unsigned(mode) >= percpu_arena_mode_enabled_base;
}

/// jemalloc: percpu_arena_mode_names = {"percpu", "phycpu", "disabled", "percpu", "phycpu"}
extern const char * const percpu_arena_mode_names[5];

/// The `metadata_thp` option (`MetadataThpMode`, `metadata_thp_mode_limit`, `metadata_thp_mode_names` are defined in
/// Pages.h), the `thp` option (`ThpMode`, `thp_mode_names_limit`, `thp_mode_names` in Pages.h) and the `dss` option
/// (`DssPrec`, `DSS_DEFAULT`, `dss_prec_names` in ExtentHooks.h). Declared opaquely here to keep this header light.
enum class MetadataThpMode : unsigned;
enum class ThpMode : unsigned;
enum class DssPrec : unsigned;

/// The current default DSS precedence for new arenas (`dss` option, `arena.<MALLCTL_ARENAS_ALL>.dss`).
/// `Disabled` when the platform has no DSS (Darwin).
/// jemalloc: extent_dss_prec_get
DssPrec extentDssPrecGet();

/// Returns true on error (a non-`Disabled` precedence on a platform without DSS).
/// jemalloc: extent_dss_prec_set
bool extentDssPrecSet(DssPrec dss_prec);

/// jemalloc: hpa_hugify_style_t (HPA is dropped; the option is only stored and reported).
enum class HpaHugifyStyle : unsigned
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
inline constexpr unsigned hpa_hugify_style_limit = 4;

/// jemalloc: hpa_hugify_style_names = {"auto", "none", "eager", "lazy"}
extern const char * const hpa_hugify_style_names[4];

/// jemalloc: prof_time_res_mode_names = {"default", "high"} (`ProfTimeRes` is defined in NsTime.h)
extern const char * const prof_time_res_mode_names[2];

/// The values of the `junk` option as reported by `opt.junk`.
inline constexpr const char * JUNK_TRUE = "true";
inline constexpr const char * JUNK_FALSE = "false";
inline constexpr const char * JUNK_ALLOC = "alloc";
inline constexpr const char * JUNK_FREE = "free";

/// --- Constants that bound option values --------------------------------------------------------------------------

/// The characters accepted by `stats_print_opts` / `stats_interval_opts`, in the order of jemalloc's
/// `STATS_PRINT_OPTIONS` (`stats.h`): json, general, merged, destroyed, unmerged, bins, large, mutex, extents, hpa.
inline constexpr char stats_print_option_chars[] = "Jgmdablxeh";
/// jemalloc: stats_print_tot_num_options
inline constexpr size_t stats_print_tot_num_options = sizeof(stats_print_option_chars) - 1;

/// jemalloc: TCACHE_LG_MAXCLASS_LIMIT, TCACHE_MAXCLASS_LIMIT, TCACHE_NBINS_MAX (`tcache_types.h`)
inline constexpr unsigned TCACHE_LG_MAXCLASS_LIMIT = LG_USIZE_GROW_SLOW_THRESHOLD;
inline constexpr size_t TCACHE_MAXCLASS_LIMIT = size_t(1) << TCACHE_LG_MAXCLASS_LIMIT;
inline constexpr unsigned TCACHE_NBINS_MAX = SC_NBINS + unsigned(SC_NGROUP) * (TCACHE_LG_MAXCLASS_LIMIT - SC_LG_LARGE_MINCLASS) + 1;

/// jemalloc: MAX_BACKGROUND_THREAD_LIMIT, DEFAULT_NUM_BACKGROUND_THREAD (`background_thread_structs.h`)
inline constexpr size_t MAX_BACKGROUND_THREAD_LIMIT = MALLOCX_ARENA_LIMIT;
inline constexpr size_t DEFAULT_NUM_BACKGROUND_THREAD = 4;

/// jemalloc: PROF_BT_MAX_LIMIT (not `JEMALLOC_PROF_GCC`), PROF_DUMP_FILENAME_LEN (`prof_types.h`)
inline constexpr unsigned PROF_BT_MAX_LIMIT = UINT_MAX;
inline constexpr size_t PROF_DUMP_FILENAME_LEN = PATH_MAX + 1;

/// jemalloc: PROCESS_MADVISE_MAX_BATCH_LIMIT (`JEMALLOC_HAVE_PROCESS_MADVISE` is not defined on any platform).
inline constexpr size_t PROCESS_MADVISE_MAX_BATCH_LIMIT = 0;

/// jemalloc: SEC_OPTS_* (`sec_opts.h`)
inline constexpr size_t SEC_OPTS_NSHARDS_DEFAULT = 2;
inline constexpr size_t SEC_OPTS_BATCH_FILL_EXTRA_DEFAULT = 3;
inline constexpr size_t SEC_OPTS_MAX_ALLOC_DEFAULT = (32 * 1024) < PAGE ? PAGE : (32 * 1024);
inline constexpr size_t SEC_OPTS_MAX_BYTES_DEFAULT
    = (256 * 1024) < (4 * SEC_OPTS_MAX_ALLOC_DEFAULT) ? (4 * SEC_OPTS_MAX_ALLOC_DEFAULT) : (256 * 1024);

/// jemalloc: HUGEPAGE_PAGES (`pages.h`)
inline constexpr size_t HUGEPAGE_PAGES = HUGEPAGE / PAGE;

/// --- Option groups -----------------------------------------------------------------------------------------------

/// HPA options (dropped feature: stored and reported only).
/// jemalloc: hpa_shard_opts_t, HPA_SHARD_OPTS_DEFAULT (`hpa_opts.h`)
struct HpaShardOpts
{
    size_t slab_max_alloc = 64 * 1024;
    size_t hugification_threshold = HUGEPAGE * 95 / 100;
    FixedPoint dirty_mult = fxp::initPercent(25);
    bool deferral_allowed = false;
    uint64_t hugify_delay_ms = 10 * 1000;
    bool hugify_sync = false;
    uint64_t min_purge_interval_ms = 5 * 1000;
    ssize_t experimental_max_purge_nhp = -1;
    size_t purge_threshold = PAGE;
    uint64_t min_purge_delay_ms = 0;
    HpaHugifyStyle hugify_style = HpaHugifyStyle::Lazy;
};

/// SEC options (dropped feature: stored and reported only).
/// jemalloc: sec_opts_t, SEC_OPTS_DEFAULT (`sec_opts.h`)
struct SecOpts
{
    size_t nshards = SEC_OPTS_NSHARDS_DEFAULT;
    size_t max_alloc = SEC_OPTS_MAX_ALLOC_DEFAULT;
    size_t max_bytes = SEC_OPTS_MAX_BYTES_DEFAULT;
    size_t batch_fill_extra = SEC_OPTS_BATCH_FILL_EXTRA_DEFAULT;
};

struct Options
{
    /// --- jemalloc.c ---

    /// The `/etc/malloc.conf` symlink target (never read in ClickHouse's configuration, so always null).
    /// jemalloc: opt_malloc_conf_symlink
    const char * malloc_conf_symlink = nullptr;
    /// The value of `MALLOC_CONF` at initialization, if set. jemalloc: opt_malloc_conf_env_var
    const char * malloc_conf_env_var = nullptr;

    /// jemalloc: opt_abort (true only with `JEMALLOC_DEBUG`)
    bool abort = false;
    /// jemalloc: opt_abort_conf (true only with `JEMALLOC_DEBUG`)
    bool abort_conf = false;
    /// Intentionally default off, even with debug builds. jemalloc: opt_confirm_conf
    bool confirm_conf = false;
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
    /// Never settable in ClickHouse's configuration (`config_utrace`, `config_xmalloc`, `config_enable_cxx` are
    /// false; s390x and FreeBSD ppc64le have `JEMALLOC_ENABLE_CXX`, but only `experimental_infallible_new` depends on it).
    /// jemalloc: opt_utrace, opt_xmalloc, opt_experimental_infallible_new
    bool utrace = false;
    bool xmalloc = false;
    bool experimental_infallible_new = false;
    /// jemalloc: opt_experimental_tcache_gc
    bool experimental_tcache_gc = true;
    /// jemalloc: opt_zero
    bool zero = false;
    /// 0 means "computed at boot" (`malloc_init_narenas` replaces it). jemalloc: opt_narenas
    unsigned narenas = 0;
    /// jemalloc: opt_narenas_ratio
    FixedPoint narenas_ratio = fxp::initInt(4);
    /// Forced to 0 after parsing (jemalloc's `config_debug` is false). jemalloc: opt_debug_double_free_max_scan
    unsigned debug_double_free_max_scan = 32; /// SAFETY_CHECK_DOUBLE_FREE_MAX_SCAN_DEFAULT
    /// jemalloc: opt_calloc_madvise_threshold (CALLOC_MADVISE_THRESHOLD_DEFAULT)
    size_t calloc_madvise_threshold = size_t(1) << 23;

    /// --- HPA / SEC (dropped: parsed, stored, reported; `hpa` is reset to false at boot, see `hpaDisableUnsupported`) ---

    /// jemalloc: opt_hpa
    bool hpa = false;
    /// jemalloc: opt_hpa_opts
    HpaShardOpts hpa_opts;
    /// jemalloc: opt_hpa_sec_opts
    SecOpts hpa_sec_opts;
    /// jemalloc: opt_experimental_hpa_start_huge_if_thp_always (`hpa.c`)
    bool experimental_hpa_start_huge_if_thp_always = true;
    /// jemalloc: opt_experimental_hpa_enforce_hugify (`hpa.c`)
    bool experimental_hpa_enforce_hugify = false;

    /// --- arena.c ---

    /// jemalloc: opt_percpu_arena (PERCPU_ARENA_DEFAULT)
    PercpuArenaMode percpu_arena = PercpuArenaMode::Disabled;
    /// jemalloc: opt_dirty_decay_ms (DIRTY_DECAY_MS_DEFAULT)
    ssize_t dirty_decay_ms = 10 * 1000;
    /// jemalloc: opt_muzzy_decay_ms (MUZZY_DECAY_MS_DEFAULT)
    ssize_t muzzy_decay_ms = 0;
    /// Allocations of at least this size use the dedicated huge arena; 0 disables. jemalloc: opt_oversize_threshold
    size_t oversize_threshold = size_t(8) << 20; /// OVERSIZE_THRESHOLD_DEFAULT
    /// jemalloc: opt_huge_arena_pac_thp
    bool huge_arena_pac_thp = false;

    /// --- base.c, pages.c, extent_mmap.c, extent_dss.c, extent.c ---

    /// jemalloc: opt_metadata_thp (METADATA_THP_DEFAULT)
    MetadataThpMode metadata_thp = MetadataThpMode(0); /// MetadataThpMode::Disabled
    /// Set to `NotSupported` by the pages boot when THP is unavailable. jemalloc: opt_thp
    ThpMode thp = ThpMode(0); /// THP_MODE_DEFAULT = ThpMode::DoNothing
    /// jemalloc: opt_retain (`JEMALLOC_RETAIN`)
    bool retain = config::retain;
    /// One of `dss_prec_names`. jemalloc: opt_dss
    const char * dss = "secondary"; /// DSS_DEFAULT
    /// jemalloc: opt_lg_extent_max_active_fit (LG_EXTENT_MAX_ACTIVE_FIT_DEFAULT)
    size_t lg_extent_max_active_fit = 6;
    /// jemalloc: opt_process_madvise_max_batch (0 without `JEMALLOC_HAVE_PROCESS_MADVISE`)
    size_t process_madvise_max_batch = 0;

    /// --- mutex.c ---

    /// Spin iterations before blocking; -1 spins forever. jemalloc: opt_mutex_max_spin
    int64_t mutex_max_spin = 600;

    /// --- stats.c ---

    /// jemalloc: opt_stats_print
    bool stats_print = false;
    /// jemalloc: opt_stats_print_opts
    char stats_print_opts[stats_print_tot_num_options + 1] = "";
    /// jemalloc: opt_stats_interval (STATS_INTERVAL_DEFAULT)
    int64_t stats_interval = -1;
    /// jemalloc: opt_stats_interval_opts
    char stats_interval_opts[stats_print_tot_num_options + 1] = "";

    /// --- tcache.c ---

    /// jemalloc: opt_tcache
    bool tcache = true;
    /// jemalloc: opt_tcache_max
    size_t tcache_max = size_t(1) << 15;
    /// jemalloc: opt_tcache_nslots_small_min, opt_tcache_nslots_small_max, opt_tcache_nslots_large
    unsigned tcache_nslots_small_min = 20;
    unsigned tcache_nslots_small_max = 200;
    unsigned tcache_nslots_large = 20;
    /// jemalloc: opt_lg_tcache_nslots_mul
    ssize_t lg_tcache_nslots_mul = 1;
    /// jemalloc: opt_tcache_gc_incr_bytes
    size_t tcache_gc_incr_bytes = 65536;
    /// jemalloc: opt_tcache_gc_delay_bytes
    size_t tcache_gc_delay_bytes = 0;
    /// jemalloc: opt_lg_tcache_flush_small_div, opt_lg_tcache_flush_large_div
    unsigned lg_tcache_flush_small_div = 1;
    unsigned lg_tcache_flush_large_div = 1;
    /// The per-bin `ncached_max` set by `tcache_ncached_max` (`cache_bin_info_t::ncached_max` values; the tcache boot
    /// fills in the bins that were not set), and which bins were set.
    /// jemalloc: opt_tcache_ncached_max, opt_tcache_ncached_max_set (static in `tcache.c`)
    uint16_t tcache_ncached_max[TCACHE_NBINS_MAX] = {};
    bool tcache_ncached_max_set[TCACHE_NBINS_MAX] = {};

    /// --- background_thread.c ---

    /// jemalloc: opt_background_thread (BACKGROUND_THREAD_DEFAULT)
    bool background_thread = false;
    /// The background thread boot replaces values above `MAX_BACKGROUND_THREAD_LIMIT` with
    /// `DEFAULT_NUM_BACKGROUND_THREAD`. jemalloc: opt_max_background_threads
    size_t max_background_threads = MAX_BACKGROUND_THREAD_LIMIT + 1;

    /// --- prof.c, prof_log.c, prof_recent.c, prof_stats.c, nstime.c ---

    /// jemalloc: opt_prof
    bool prof = false;
    /// jemalloc: opt_prof_active
    bool prof_active = true;
    /// jemalloc: opt_prof_thread_active_init
    bool prof_thread_active_init = true;
    /// jemalloc: opt_prof_bt_max (PROF_BT_MAX_DEFAULT)
    unsigned prof_bt_max = 128;
    /// jemalloc: opt_lg_prof_sample (LG_PROF_SAMPLE_DEFAULT)
    size_t lg_prof_sample = 19;
    /// jemalloc: opt_lg_prof_interval (LG_PROF_INTERVAL_DEFAULT)
    ssize_t lg_prof_interval = -1;
    /// jemalloc: opt_prof_gdump, opt_prof_final, opt_prof_leak, opt_prof_leak_error, opt_prof_accum
    bool prof_gdump = false;
    bool prof_final = false;
    bool prof_leak = false;
    bool prof_leak_error = false;
    bool prof_accum = false;
    /// jemalloc: opt_prof_pid_namespace
    bool prof_pid_namespace = false;
    /// Initialized with PROF_PREFIX_DEFAULT here (jemalloc does it in `prof_boot0`, before parsing the options).
    /// jemalloc: opt_prof_prefix
    char prof_prefix[PROF_DUMP_FILENAME_LEN] = "jeprof";
    /// jemalloc: opt_prof_sys_thread_name
    bool prof_sys_thread_name = false;
    /// jemalloc: opt_prof_unbias
    bool prof_unbias = true;
    /// Dropped feature (`prof_log`): stored and reported only. jemalloc: opt_prof_log
    bool prof_log = false;
    /// jemalloc: opt_prof_recent_alloc_max (PROF_RECENT_ALLOC_MAX_DEFAULT)
    ssize_t prof_recent_alloc_max = 0;
    /// jemalloc: opt_prof_stats
    bool prof_stats = false;
    /// Which clock `NsTime::profUpdate` uses. jemalloc: opt_prof_time_res (`nstime.c`)
    ProfTimeRes prof_time_res = ProfTimeRes::Default;

    /// --- san.c ---

    /// jemalloc: opt_san_guard_large, opt_san_guard_small (SAN_GUARD_*_EVERY_N_EXTENTS_DEFAULT)
    size_t san_guard_large = 0;
    size_t san_guard_small = 0;
    /// Only settable with `config::uaf_detection`. jemalloc: opt_lg_san_uaf_align (SAN_LG_UAF_ALIGN_DEFAULT)
    ssize_t lg_san_uaf_align = -1;
};

/// jemalloc: all `opt_*` globals.
extern constinit Options opt;

}
