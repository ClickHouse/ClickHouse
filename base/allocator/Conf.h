#pragma once

/// The run-time configuration parser: reads the `malloc_conf` sources and sets the fields of `opt` (Options.h).
/// jemalloc: `conf.h`, `src/conf.c`, `multi_setting_parse_next` from `src/util.c`.
///
/// Sources, in order (a later one overrides an earlier one for the same option):
///   1. the compiled-in string `config::malloc_conf_default` (`--with-malloc-conf`, ClickHouse's CMake);
///   2. the weak global `je_malloc_conf` (null unless the application defines it);
///   3. the `/etc/je_malloc.conf` symlink target - never read: ClickHouse does not define `JEMALLOC_CONFIG_FILE`;
///   4. the `MALLOC_CONF` environment variable (`secure_getenv` where available; ignored in set-uid/set-gid programs
///      where `issetugid` exists);
///   5. the weak global `je_malloc_conf_2_conf_harder`.
///
/// The parser makes two passes over all sources: the first one only applies `confirm_conf`; the second one applies
/// everything and reports errors as `<jemalloc>: ...` messages through `je_malloc_message`. With `abort_conf:true`
/// an error aborts the process after the source that contained it (or a later one) has been processed.
///
/// Options with side effects call into the owning subsystem: `slab_sizes` updates the size class data, `bin_shards`
/// the bin shard counts (SizeClasses.h), `tcache_ncached_max` fills `opt.tcache_ncached_max`, `dss` sets the DSS
/// precedence (Options.h). Everything else is only stored in `opt`.
///
/// Deviation from jemalloc: HPA is never supported (dropped feature). `hpa:true` is accepted and stored, and
/// `hpaDisableUnsupported` (called by the initialization where jemalloc checks `hpa_supported`) prints jemalloc's
/// "HPA not supported in the current configuration" message and resets `opt.hpa`, as jemalloc itself does on
/// platforms without HPA support (e.g. Linux aarch64 with 64 KiB pages, where the huge page is too large, or without
/// THP); on x86_64 with THP jemalloc would enable HPA instead.

#include <allocator/Common.h>
#include <allocator/Options.h>

#include <climits>
#include <cstddef>
#include <cstdint>

/// The application may define these to configure the allocator (weak definitions with null values are in Conf.cpp).
/// jemalloc: je_malloc_conf, je_malloc_conf_2_conf_harder
extern "C" __attribute__((visibility("default"))) const char * je_malloc_conf;
extern "C" __attribute__((visibility("default"))) const char * je_malloc_conf_2_conf_harder;

namespace jemalloc
{

struct SizeClassData;

/// Number of sources for initializing malloc_conf. jemalloc: MALLOC_CONF_NSOURCES
inline constexpr unsigned MALLOC_CONF_NSOURCES = 5;

/// Size of the buffer for the symlink target (jemalloc: `char readlink_buf[PATH_MAX + 1]`).
inline constexpr size_t MALLOC_CONF_READLINK_BUF_SIZE = PATH_MAX + 1;

/// Whether any invalid configuration option was encountered (sticky).
/// jemalloc: had_conf_error
extern bool had_conf_error;

/// Parse all configuration sources and set `opt`, `sc_data` (slab sizes) and `bin_shard_sizes` (`SC_NBINS` entries).
/// Must be called after `scBoot(sc_data)` and `binShardSizesBoot(bin_shard_sizes)`. `readlink_buf` receives the
/// `/etc/malloc.conf` symlink target (always the empty string in ClickHouse's configuration); it must be
/// `MALLOC_CONF_READLINK_BUF_SIZE` bytes with `readlink_buf[0] == '\0'`.
/// jemalloc: malloc_conf_init
void mallocConfInit(SizeClassData & sc_data, unsigned * bin_shard_sizes, char * readlink_buf);

/// Print the abort message and abort.
/// jemalloc: malloc_abort_invalid_conf
[[noreturn]] void mallocAbortInvalidConf();

/// The check that `malloc_init_hard_a0_locked` does after `prof_boot1` (and again after creating arena 0):
/// `if (opt_hpa && !hpa_supported())` print "<jemalloc>: HPA not supported in the current configuration; disabling."
/// (or "aborting." and abort with `abort_conf`) and reset `opt.hpa`. HPA is never supported here (see above).
/// jemalloc: the `hpa_supported` check in `malloc_init_hard_a0_locked`
void hpaDisableUnsupported();

/// Whether HPA can be used. Always false: HPA is a dropped feature.
/// jemalloc: hpa_supported
constexpr bool hpaSupported()
{
    return false;
}

/// --- Parsing primitives (exposed for tests) ----------------------------------------------------------------------

/// Extract the next `key:value` pair. Returns true at the end of the string or on a syntax error (the error is
/// printed and `had_conf_error` is set).
/// jemalloc: conf_next
bool confNext(const char ** opts_p, const char ** k_p, size_t * klen_p, const char ** v_p, size_t * vlen_p);

/// Print "<jemalloc>: <msg>: <k>:<v>" and set `had_conf_error` (unless the key is experimental or deprecated).
/// jemalloc: conf_error
void confError(const char * msg, const char * k, size_t klen, const char * v, size_t vlen);

/// Exactly "true" or "false". Returns true on error.
/// jemalloc: conf_handle_bool
bool confHandleBool(const char * v, size_t vlen, bool * result);

/// Returns true on error (not a number, trailing characters, or out of range without `clip`).
/// jemalloc: conf_handle_unsigned
bool confHandleUnsigned(
    const char * v, size_t vlen, uintmax_t min, uintmax_t max, bool check_min, bool check_max, bool clip, uintmax_t * result);

/// jemalloc: conf_handle_signed
bool confHandleSigned(
    const char * v, size_t vlen, intmax_t min, intmax_t max, bool check_min, bool check_max, bool clip, intmax_t * result);

/// Copy at most `dest_sz - 1` characters and NUL-terminate. Never fails (returns false).
/// jemalloc: conf_handle_char_p
bool confHandleCharP(const char * v, size_t vlen, char * dest, size_t dest_sz);

/// Parse one `start-end:value` segment (numbers in any `strtoumax` base) followed by an optional `|`, advancing
/// `*setting_segment_cur` and decreasing `*len_left`. Returns true on error.
/// jemalloc: multi_setting_parse_next
bool multiSettingParseNext(const char ** setting_segment_cur, size_t * len_left, size_t * key_start, size_t * key_end, size_t * value);

/// Parse `start-end:ncached_max[|...]` and call `set(bin_index, ncached_max)` for every tcache bin in the ranges
/// (ranges are clipped to `TCACHE_MAXCLASS_LIMIT`, empty ranges are skipped, `ncached_max` is clipped to
/// `CACHE_BIN_NCACHED_MAX`). Returns true on a syntax error (the bins of earlier segments stay updated).
/// Used for the `tcache_ncached_max` option and for `thread.tcache.ncached_max.write`.
/// jemalloc: tcache_bin_info_settings_parse
template <typename SetNcachedMax>
bool tcacheBinInfoSettingsParse(const char * bin_settings_segment_cur, size_t len_left, SetNcachedMax && set);

/// The `tcache_ncached_max` option: `tcacheBinInfoSettingsParse` into `opt.tcache_ncached_max` /
/// `opt.tcache_ncached_max_set`. Returns true on error.
/// jemalloc: tcache_bin_info_default_init
bool tcacheBinInfoDefaultInit(const char * bin_settings_segment_cur, size_t len_left);

}

/// --- Implementation of templates -----------------------------------------------------------------------------------

#include <allocator/SizeClasses.h>

namespace jemalloc
{

/// The maximum number of items in a cache bin (`cache_bin_sz_t` is 16 bits). jemalloc: CACHE_BIN_NCACHED_MAX
inline constexpr size_t CONF_CACHE_BIN_NCACHED_MAX = ((size_t(1) << (sizeof(uint16_t) * 8)) / sizeof(void *)) - 1;

template <typename SetNcachedMax>
bool tcacheBinInfoSettingsParse(const char * bin_settings_segment_cur, size_t len_left, SetNcachedMax && set)
{
    do
    {
        size_t size_start;
        size_t size_end;
        size_t ncached_max;
        bool err = multiSettingParseNext(&bin_settings_segment_cur, &len_left, &size_start, &size_end, &ncached_max);
        if (err)
            return true;
        if (size_end > TCACHE_MAXCLASS_LIMIT)
            size_end = TCACHE_MAXCLASS_LIMIT;
        if (size_start > TCACHE_MAXCLASS_LIMIT || size_start > size_end)
            continue;
        /// May get called before sz_init (during malloc_conf_init).
        szind_t bin_start = sz::sizeToIndexCompute(size_start);
        szind_t bin_end = sz::sizeToIndexCompute(size_end);
        if (ncached_max > CONF_CACHE_BIN_NCACHED_MAX)
            ncached_max = CONF_CACHE_BIN_NCACHED_MAX;
        for (szind_t i = bin_start; i <= bin_end; ++i)
            set(i, static_cast<uint16_t>(ncached_max));
    } while (len_left > 0);

    return false;
}

}
