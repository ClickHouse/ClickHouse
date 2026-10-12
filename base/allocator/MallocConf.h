#pragma once

/// The run-time configuration parser: reads the `malloc_conf` sources and sets the fields of `options` (Options.h).
/// jemalloc: `conf.h`, `src/conf.c`, `multi_setting_parse_next` from `src/util.c`.
///
/// Sources, in order (a later one overrides an earlier one for the same option):
///   1. the compiled-in string `config::malloc_conf_default` (`--with-malloc-configuration`, ClickHouse's CMake);
///   2. the weak global `je_malloc_conf` (null unless the application defines it);
///   3. the `/etc/je_malloc.configuration` symlink target - never read: ClickHouse does not define `JEMALLOC_CONFIG_FILE`;
///   4. the `MALLOC_CONF` environment variable (`secure_getenv` where available; ignored in set-uid/set-gid programs
///      where `issetugid` exists);
///   5. the weak global `je_malloc_conf_2_conf_harder`.
///
/// The parser makes two passes over all sources: the first one only applies `confirm_configuration`; the second one applies
/// everything and reports errors as `<jemalloc>: ...` messages through `je_malloc_message`. With `abort_configuration:true`
/// an error aborts the process after the source that contained it (or a later one) has been processed.
///
/// Options with side effects call into the owning subsystem: `slab_sizes` updates the size class data, `bin_shards`
/// the bin shard counts (SizeClasses.h), `thread_cache_num_cached_max` fills `opt.tcache_ncached_max`, `sbrk` sets the DSS
/// precedence (Options.h). Everything else is only stored in `options`.
///
/// Deviation from jemalloc: HPA is never supported (dropped feature). `hpa:true` is accepted and stored, and
/// `hugePageAllocatorDisableUnsupported` (called by the initialization where jemalloc checks `hpa_supported`) prints jemalloc's
/// "HPA not supported in the current configuration" message and resets `opt.hpa`, as jemalloc itself does on
/// platforms without HPA support (e.g. Linux aarch64 with 64 KiB pages, where the huge page is too large, or without
/// THP); on x86_64 with THP jemalloc would enable HPA instead.

#include <allocator/Common.h>
#include <allocator/Options.h>

#include <climits>
#include <cstddef>
#include <cstdint>

/// The application may define these to configure the allocator (weak definitions with null values are in MallocConf.cpp).
/// jemalloc: je_malloc_conf, je_malloc_conf_2_conf_harder
extern "C" __attribute__((visibility("default"))) const char * je_malloc_conf;
extern "C" __attribute__((visibility("default"))) const char * je_malloc_conf_2_conf_harder;

namespace jemalloc
{

struct SizeClassData;

/// Number of sources for initializing malloc_conf. jemalloc: MALLOC_CONF_NSOURCES
inline constexpr unsigned MALLOC_CONF_NUM_SOURCES = 5;

/// Size of the buffer for the symlink target (jemalloc: `char readlink_buf[PATH_MAX + 1]`).
inline constexpr size_t MALLOC_CONF_READLINK_BUF_SIZE = PATH_MAX + 1;

/// Whether any invalid configuration option was encountered (sticky).
/// jemalloc: had_conf_error
extern bool had_configuration_error;

/// Parse all configuration sources and set `options`, `size_class_data` (slab sizes) and `bin_shard_sizes` (`SIZE_CLASS_NUM_BINS` entries).
/// Must be called after `sizeClassBoot(size_class_data)` and `binShardSizesBoot(bin_shard_sizes)`. `readlink_buf` receives the
/// `/etc/malloc.configuration` symlink target (always the empty string in ClickHouse's configuration); it must be
/// `MALLOC_CONF_READLINK_BUF_SIZE` bytes with `readlink_buf[0] == '\0'`.
/// jemalloc: malloc_conf_init
void mallocConfInit(SizeClassData & size_class_data, unsigned * bin_shard_sizes, char * readlink_buf);

/// Print the abort message and abort.
/// jemalloc: malloc_abort_invalid_conf
[[noreturn]] void mallocAbortInvalidConfiguration();

/// The check that `malloc_init_hard_a0_locked` does after `prof_boot1` (and again after creating arena 0):
/// `if (opt_hpa && !hpa_supported())` print "<jemalloc>: HPA not supported in the current configuration; disabling."
/// (or "aborting." and abort with `abort_configuration`) and reset `opt.hpa`. HPA is never supported here (see above).
/// jemalloc: the `hpa_supported` check in `malloc_init_hard_a0_locked`
void hugePageAllocatorDisableUnsupported();

/// Whether HPA can be used. Always false: HPA is a dropped feature.
/// jemalloc: hpa_supported
constexpr bool hugePageAllocatorSupported()
{
    return false;
}

/// --- Parsing primitives (exposed for tests) ----------------------------------------------------------------------

/// Extract the next `key:value` pair. Returns true at the end of the string or on a syntax error (the error is
/// printed and `had_configuration_error` is set).
/// jemalloc: conf_next
bool configurationNext(
    const char ** options_string_ptr, const char ** key_ptr, size_t * key_length_ptr, const char ** value_ptr, size_t * value_length_ptr);

/// Print "<jemalloc>: <msg>: <k>:<v>" and set `had_configuration_error` (unless the key is experimental or deprecated).
/// jemalloc: conf_error
void configurationError(const char * message, const char * k, size_t key_length, const char * v, size_t value_length);

/// Exactly "true" or "false". Returns true on error.
/// jemalloc: conf_handle_bool
bool configurationHandleBool(const char * v, size_t value_length, bool * result);

/// Returns true on error (not a number, trailing characters, or out of range without `clip`).
/// jemalloc: conf_handle_unsigned
bool configurationHandleUnsigned(
    const char * v, size_t value_length, uintmax_t min, uintmax_t max, bool check_min, bool check_max, bool clip, uintmax_t * result);

/// jemalloc: conf_handle_signed
bool configurationHandleSigned(
    const char * v, size_t value_length, intmax_t min, intmax_t max, bool check_min, bool check_max, bool clip, intmax_t * result);

/// Copy at most `destination_size - 1` characters and NUL-terminate. Never fails (returns false).
/// jemalloc: conf_handle_char_p
bool configurationHandleCharPtr(const char * v, size_t value_length, char * destination, size_t destination_size);

/// Parse one `start-end:value` segment (numbers in any `strtoumax` base) followed by an optional `|`, advancing
/// `*setting_segment_current` and decreasing `*len_left`. Returns true on error.
/// jemalloc: multi_setting_parse_next
bool multiSettingParseNext(const char ** setting_segment_current, size_t * len_left, size_t * key_start, size_t * key_end, size_t * value);

/// Parse `start-end:num_cached_max[|...]` and call `set(bin_index, num_cached_max)` for every tcache bin in the ranges
/// (ranges are clipped to `THREAD_CACHE_MAX_CLASS_LIMIT`, empty ranges are skipped, `num_cached_max` is clipped to
/// `CACHE_BIN_NUM_CACHED_MAX`). Returns true on a syntax error (the bins of earlier segments stay updated).
/// Used for the `tcache_ncached_max` option and for `thread.tcache.ncached_max.write`.
/// jemalloc: tcache_bin_info_settings_parse
template <typename SetNumCachedMax>
bool threadCacheBinInfoSettingsParse(const char * bin_settings_segment_current, size_t len_left, SetNumCachedMax && set);

/// The `tcache_ncached_max` option: `threadCacheBinInfoSettingsParse` into `opt.tcache_ncached_max` /
/// `opt.tcache_ncached_max_set`. Returns true on error.
/// jemalloc: tcache_bin_info_default_init
bool threadCacheBinInfoDefaultInit(const char * bin_settings_segment_current, size_t len_left);

}

/// --- Implementation of templates -----------------------------------------------------------------------------------

#include <allocator/SizeClasses.h>

namespace jemalloc
{

/// The maximum number of items in a cache bin (`CacheBinSize` is 16 bits). jemalloc: CACHE_BIN_NCACHED_MAX
inline constexpr size_t CONFIGURATION_CACHE_BIN_NUM_CACHED_MAX = ((size_t(1) << (sizeof(uint16_t) * 8)) / sizeof(void *)) - 1;

template <typename SetNumCachedMax>
bool threadCacheBinInfoSettingsParse(const char * bin_settings_segment_current, size_t len_left, SetNumCachedMax && set)
{
    do
    {
        size_t size_start;
        size_t size_end;
        size_t num_cached_max;
        bool error = multiSettingParseNext(&bin_settings_segment_current, &len_left, &size_start, &size_end, &num_cached_max);
        if (error)
            return true;
        if (size_end > THREAD_CACHE_MAX_CLASS_LIMIT)
            size_end = THREAD_CACHE_MAX_CLASS_LIMIT;
        if (size_start > THREAD_CACHE_MAX_CLASS_LIMIT || size_start > size_end)
            continue;
        /// May get called before sz_init (during malloc_conf_init).
        SizeClassIdx bin_start = size_classes::sizeToIndexCompute(size_start);
        SizeClassIdx bin_end = size_classes::sizeToIndexCompute(size_end);
        if (num_cached_max > CONFIGURATION_CACHE_BIN_NUM_CACHED_MAX)
            num_cached_max = CONFIGURATION_CACHE_BIN_NUM_CACHED_MAX;
        for (SizeClassIdx i = bin_start; i <= bin_end; ++i)
            set(i, static_cast<uint16_t>(num_cached_max));
    } while (len_left > 0);

    return false;
}

}
