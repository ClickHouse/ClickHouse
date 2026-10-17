#include <allocator/MallocConf.h>

#include <allocator/ExtentHooks.h>
#include <allocator/FixedPoint.h>
#include <allocator/Format.h>
#include <allocator/Nanoseconds.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <unistd.h>

/// Weak, so that the application can provide its own definitions; zero-initialized like jemalloc's tentative
/// definitions `const char *je_malloc_conf JEMALLOC_ATTR(weak);`.
extern "C" __attribute__((weak, visibility("default"))) const char * je_malloc_conf = nullptr;
extern "C" __attribute__((weak, visibility("default"))) const char * je_malloc_conf_2_conf_harder = nullptr;

namespace jemalloc
{

constinit bool had_configuration_error = false;

namespace
{

/// ClickHouse defines `JEMALLOC_CONFIG_ENV` but not `JEMALLOC_CONFIG_FILE` (`jemalloc-cmake/include/jemalloc/jemalloc_defs.h`),
/// so the `/etc/je_malloc.configuration` symlink is never read.
constexpr bool config_file = false;

/// `JEMALLOC_HAVE_CLOCK_REALTIME` is defined on all platforms.
constexpr bool config_high_resolution_timer = true;

/// `JEMALLOC_HAVE_MEMCNTL` is not defined on any platform.
constexpr bool have_memcntl = false;

/// `JEMALLOC_HAVE_MADVISE_COLLAPSE` is not defined on any platform.
constexpr bool have_madvise_collapse = false;

/// jemalloc: HUGEPAGE_MAX_EXPECTED_SIZE (`pages.h`)
constexpr size_t HUGE_PAGE_MAX_EXPECTED_SIZE = size_t(16) << 20;

/// jemalloc: jemalloc_getenv
const char * jemallocGetenv(const char * name)
{
#if defined(__linux__) && !defined(ALLOCATOR_MUSL)
    if constexpr (config::have_secure_getenv)
        return secure_getenv(name);
#endif
#if defined(__FreeBSD__) || defined(__APPLE__)
    if constexpr (config::have_issetugid)
    {
        if (issetugid() != 0)
            return nullptr;
    }
#endif
    return getenv(name);
}

/// jemalloc: init_opt_stats_opts
void initOptionStatsOptions(const char * v, size_t value_length, char * destination)
{
    size_t options_len = strlen(destination);
    ALLOCATOR_ASSERT(options_len <= stats_print_total_num_options);

    for (size_t i = 0; i < value_length; ++i)
    {
        if (v[i] == '\0' || strchr(stats_print_option_chars, v[i]) == nullptr)
            continue;

        if (strchr(destination, v[i]) != nullptr)
        {
            /// Ignore repeated.
            continue;
        }

        destination[options_len++] = v[i];
        destination[options_len] = '\0';
        ALLOCATOR_ASSERT(options_len <= stats_print_total_num_options);
    }
    ALLOCATOR_ASSERT(options_len == strlen(destination));
}

/// jemalloc: malloc_conf_format_error
void mallocConfFormatError(const char * message, const char * begin, const char * end)
{
    size_t len = size_t(end - begin + 1);
    len = len > BUF_ERROR_BUF ? BUF_ERROR_BUF : len;

    printMessage("<jemalloc>: %s -- %.*s\n", message, int(len), begin);
}

/// jemalloc: obtain_malloc_conf
const char * obtainMallocConf(unsigned which_source, char * readlink_buf)
{
    ALLOCATOR_ASSERT(which_source < MALLOC_CONF_NUM_SOURCES);

    const char * result;
    switch (which_source)
    {
        case 0: result = config::malloc_conf_default; break;
        case 1:
            if (je_malloc_conf != nullptr)
            {
                /// Use options that were compiled into the program.
                result = je_malloc_conf;
            }
            else
            {
                /// No configuration specified.
                result = nullptr;
            }
            break;
        case 2: {
            if constexpr (!config_file)
            {
                result = nullptr;
                break;
            }
            else
            {
                ssize_t link_length = 0;
                int saved_errno = errno;
                const char * link_name = "/etc/je_malloc.conf";

                /// Try to use the contents of the "/etc/malloc.conf" symbolic link's name.
                link_length = readlink(link_name, readlink_buf, PATH_MAX);
                if (link_length == -1)
                {
                    /// No configuration specified.
                    link_length = 0;
                    /// Restore errno.
                    errno = saved_errno;
                }
                readlink_buf[link_length] = '\0';
                result = readlink_buf;
                break;
            }
        }
        case 3: {
            const char * env_name = "MALLOC_CONF";
            if ((result = jemallocGetenv(env_name)) != nullptr)
                options.malloc_conf_env_variable = result;
            else
            {
                /// No configuration specified.
                result = nullptr;
            }
            break;
        }
        case 4: result = je_malloc_conf_2_conf_harder; break;
        default: ALLOCATOR_NOT_REACHED();
    }
    return result;
}

/// jemalloc: validate_hpa_settings
void validateHugePageAllocatorSettings()
{
    if (!hugePageAllocatorSupported() || !options.huge_page_allocator)
        return;
    if (HUGE_PAGE > HUGE_PAGE_MAX_EXPECTED_SIZE)
    {
        had_configuration_error = true;
        printMessage("<jemalloc>: huge page size (%zu) greater than expected.May not be supported or behave as expected.", HUGE_PAGE);
    }
    if (!have_madvise_collapse && options.huge_page_allocator_options.hugify_sync)
    {
        had_configuration_error = true;
        printMessage("<jemalloc>: hpa_hugify_sync config option is enabled, but MADV_COLLAPSE support was not detected at build time.");
    }
}

/// jemalloc: malloc_conf_init_check_deps. Returns true if there is an inconsistency.
bool mallocConfInitCheckDependencies()
{
    if (options.profiling_leak_error && !options.profiling_final)
    {
        printMessage("<jemalloc>: prof_leak_error is set w/o prof_final.\n");
        return true;
    }
    /// To emphasize in the stats output that opt is disabled when !debug (jemalloc's `config_debug` is always false).
    options.debug_double_free_max_scan = 0;

    return false;
}

/// The handling of one `key:value` pair: the body of the parsing loop of `malloc_conf_init_helper` with its macros
/// (`CONF_MATCH`, `CONF_ERROR`, `CONF_CONTINUE`, `CONF_HANDLE_*`) as methods. Every `handle*` method returns true if
/// the key matched (the pair is then fully processed: `CONF_CONTINUE`).
class ConfigurationPair
{
public:
    ConfigurationPair(
        bool initial_call_,
        SizeClassData * size_class_data_,
        unsigned * bin_shard_sizes_,
        const char * k_,
        size_t key_length_,
        const char * v_,
        size_t value_length_)
        : initial_call(initial_call_)
        , size_class_data(size_class_data_)
        , bin_shard_sizes(bin_shard_sizes_)
        , k(k_)
        , key_length(key_length_)
        , v(v_)
        , value_length(value_length_)
    {
    }

    void process();

private:
    const bool initial_call;
    SizeClassData * const size_class_data;
    unsigned * const bin_shard_sizes;
    const char * const k;
    const size_t key_length;
    const char * const v;
    const size_t value_length;
    bool current_option_valid = true;

    /// jemalloc: CONF_MATCH
    bool match(const char * n) const { return strlen(n) == key_length && strncmp(n, k, key_length) == 0; }

    /// jemalloc: CONF_MATCH_VALUE
    bool matchValue(const char * n) const { return strlen(n) == value_length && strncmp(n, v, value_length) == 0; }

    /// The keys `metadata_thp`, `sbrk`, `per_cpu_arena`, `hpa_hugify_style` are compared with `strncmp(n, k, key_length)`, so
    /// any prefix of the name matches (including the empty key).
    bool matchKeyPrefix(const char * n) const { return strncmp(n, k, key_length) == 0; }

    /// Values of enum options are compared with `strncmp(name, v, value_length)`: any prefix of the name matches.
    bool matchValuePrefix(const char * name) const { return strncmp(name, v, value_length) == 0; }

    /// jemalloc: CONF_ERROR
    void reportError(const char * message)
    {
        if (!initial_call)
        {
            configurationError(message, k, key_length, v, value_length);
            current_option_valid = false;
        }
    }

    /// jemalloc: CONF_CONTINUE (without the `continue`)
    bool done() const
    {
        if (!initial_call && options.confirm_configuration && current_option_valid)
            printMessage("<jemalloc>: -- Set conf value: %.*s:%.*s\n", int(key_length), k, int(value_length), v);
        return true;
    }

    /// jemalloc: CONF_HANDLE_BOOL
    bool handleBool(bool & o, const char * n)
    {
        if (!match(n))
            return false;
        if (configurationHandleBool(v, value_length, &o))
            reportError("Invalid conf value");
        return done();
    }

    /// jemalloc: CONF_VALUE_READ, CONF_VALUE_READ_FAIL. Returns true on failure.
    template <typename MaxT>
    bool valueRead(MaxT & result) const
    {
        const char * end;
        errno = 0;
        result = static_cast<MaxT>(strToUIntMax(v, &end, 0));
        return errno != 0 || size_t(end - v) != value_length;
    }

    /// jemalloc: CONF_HANDLE_T with `max_t` = `intmax_t` for signed `T`, `uintmax_t` otherwise
    /// (`CONF_HANDLE_UNSIGNED`, `CONF_HANDLE_SIZE_T`, `CONF_HANDLE_INT64_T`, `CONF_HANDLE_UINT64_T`).
    template <typename T>
    bool handleInteger(T & o, const char * n, T min, T max, bool check_min, bool check_max, bool clip)
    {
        using MaxT = std::conditional_t<std::is_signed_v<T>, intmax_t, uintmax_t>;
        if (!match(n))
            return false;
        MaxT max_value;
        if (valueRead(max_value))
            reportError("Invalid conf value");
        else if (clip)
        {
            if (check_min && max_value < MaxT(min))
                o = min;
            else if (check_max && max_value > MaxT(max))
                o = max;
            else
                o = static_cast<T>(max_value);
        }
        else
        {
            if ((check_min && max_value < MaxT(min)) || (check_max && max_value > MaxT(max)))
                reportError("Out-of-range conf value");
            else
                o = static_cast<T>(max_value);
        }
        return done();
    }

    /// jemalloc: CONF_HANDLE_SSIZE_T
    bool handleSsize(ssize_t & o, const char * n, ssize_t min, ssize_t max)
    {
        return handleInteger<ssize_t>(o, n, min, max, true, true, false);
    }

    /// jemalloc: CONF_HANDLE_CHAR_P
    template <size_t N>
    bool handleCharPtr(char (&o)[N], const char * n)
    {
        if (!match(n))
            return false;
        size_t copy_length = (value_length <= N - 1) ? value_length : N - 1;
        strncpy(o, v, copy_length);
        o[copy_length] = '\0';
        return done();
    }

    /// `hpa_hugification_threshold_ratio`, `hpa_purge_threshold_ratio`.
    bool handleHugePageRatio(size_t & o, const char * n)
    {
        if (!match(n))
            return false;
        FixedPoint ratio;
        const char * end;
        bool error = fixed_point::parse(&ratio, v, &end);
        if (error || size_t(end - v) != value_length || ratio > fixed_point::initInt(1))
            reportError("Invalid conf value");
        else
            o = fixed_point::multiplyByFraction(HUGE_PAGE, ratio);
        return done();
    }

    bool handleMetadataTransparentHugePages();
    bool handleSbrk();
    bool handleNumArenas();
    bool handleNumArenasRatio();
    bool handleBinShards();
    bool handleThreadCacheNumCachedMax();
    bool handleStatsOptions(char * destination, const char * n);
    bool handleJunk();
    bool handleLog2ThreadCacheMax();
    bool handlePerCPUArena();
    bool handleHugePageAllocatorHugifyStyle();
    bool handleHugePageAllocatorDirtyMultiplier();
    bool handleSlabSizes();
    bool handleProfilingTimeResolution();
    bool handleTransparentHugePages();
    bool handleZeroRealloc();
    bool handleLog2SanitizerUseAfterFreeAlign();
};

bool ConfigurationPair::handleMetadataTransparentHugePages()
{
    if (!matchKeyPrefix("metadata_thp"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < metadata_transparent_huge_pages_mode_limit; ++m)
    {
        if (matchValuePrefix(metadata_transparent_huge_pages_mode_names[m]))
        {
            options.metadata_transparent_huge_pages = MetadataTransparentHugePagesMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleSbrk()
{
    if (!matchKeyPrefix("dss"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < unsigned(SbrkPrecedence::Limit); ++m)
    {
        if (matchValuePrefix(sbrk_precedence_names[m]))
        {
            if (extentSbrkPrecedenceSet(SbrkPrecedence(m)))
                reportError("Error setting dss");
            else
            {
                options.sbrk = sbrk_precedence_names[m];
                found = true;
                break;
            }
        }
    }
    if (!found)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleNumArenas()
{
    if (!match("narenas"))
        return false;
    if (matchValue("default"))
    {
        options.num_arenas = 0;
        return done();
    }
    return handleInteger<unsigned>(options.num_arenas, "narenas", 1, UINT_MAX, true, false, false);
}

bool ConfigurationPair::handleNumArenasRatio()
{
    if (!match("narenas_ratio"))
        return false;
    const char * end;
    bool error = fixed_point::parse(&options.num_arenas_ratio, v, &end);
    if (error || size_t(end - v) != value_length)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleBinShards()
{
    if (!match("bin_shards"))
        return false;
    const char * bin_shards_segment_current = v;
    size_t value_length_left = value_length;
    do
    {
        size_t size_start;
        size_t size_end;
        size_t num_shards;
        bool error = multiSettingParseNext(&bin_shards_segment_current, &value_length_left, &size_start, &size_end, &num_shards);
        if (error || binUpdateShardSize(bin_shard_sizes, size_start, size_end, num_shards))
        {
            reportError("Invalid settings for bin_shards");
            break;
        }
    } while (value_length_left > 0);
    return done();
}

bool ConfigurationPair::handleThreadCacheNumCachedMax()
{
    if (!match("tcache_ncached_max"))
        return false;
    if (threadCacheBinInfoDefaultInit(v, value_length))
        reportError("Invalid settings for tcache_ncached_max");
    return done();
}

bool ConfigurationPair::handleStatsOptions(char * destination, const char * n)
{
    if (!match(n))
        return false;
    initOptionStatsOptions(v, value_length, destination);
    return done();
}

bool ConfigurationPair::handleJunk()
{
    if (!match("junk"))
        return false;
    if (matchValue("true"))
    {
        options.junk = JUNK_TRUE;
        options.junk_alloc = options.junk_free = true;
    }
    else if (matchValue("false"))
    {
        options.junk = JUNK_FALSE;
        options.junk_alloc = options.junk_free = false;
    }
    else if (matchValue("alloc"))
    {
        options.junk = JUNK_ALLOC;
        options.junk_alloc = true;
        options.junk_free = false;
    }
    else if (matchValue("free"))
    {
        options.junk = JUNK_FREE;
        options.junk_alloc = false;
        options.junk_free = true;
    }
    else
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleLog2ThreadCacheMax()
{
    if (!match("lg_tcache_max"))
        return false;
    size_t m;
    if (valueRead(m))
        reportError("Invalid conf value");
    else
    {
        /// Clip if necessary.
        if (m > THREAD_CACHE_LOG2_MAX_CLASS_LIMIT)
            m = THREAD_CACHE_LOG2_MAX_CLASS_LIMIT;
        options.thread_cache_max = size_t(1) << m;
    }
    return done();
}

bool ConfigurationPair::handlePerCPUArena()
{
    if (!matchKeyPrefix("percpu_arena"))
        return false;
    bool found = false;
    for (unsigned m = per_cpu_arena_mode_names_base; m < per_cpu_arena_mode_names_limit; ++m)
    {
        if (matchValuePrefix(per_cpu_arena_mode_names[m]))
        {
            if (!config::have_per_cpu_arena)
                reportError("No getcpu support");
            options.per_cpu_arena = PerCPUArenaMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleHugePageAllocatorHugifyStyle()
{
    if (!matchKeyPrefix("hpa_hugify_style"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < huge_page_allocator_hugify_style_limit; ++m)
    {
        if (matchValuePrefix(huge_page_allocator_hugify_style_names[m]))
        {
            options.huge_page_allocator_options.hugify_style = HugePageAllocatorHugifyStyle(m);
            found = true;
            break;
        }
    }
    if (!found)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleHugePageAllocatorDirtyMultiplier()
{
    if (!match("hpa_dirty_mult"))
        return false;
    if (matchValue("-1"))
    {
        options.huge_page_allocator_options.dirty_multiplier = FixedPoint(-1);
        return done();
    }
    FixedPoint ratio;
    const char * end;
    bool error = fixed_point::parse(&ratio, v, &end);
    if (error || size_t(end - v) != value_length)
        reportError("Invalid conf value");
    else
        options.huge_page_allocator_options.dirty_multiplier = ratio;
    return done();
}

bool ConfigurationPair::handleSlabSizes()
{
    if (!match("slab_sizes"))
        return false;
    if (matchValue("default"))
    {
        sizeClassDataInit(*size_class_data);
        return done();
    }
    bool error;
    const char * slab_size_segment_current = v;
    size_t value_length_left = value_length;
    do
    {
        size_t slab_start;
        size_t slab_end;
        size_t pages;
        error = multiSettingParseNext(&slab_size_segment_current, &value_length_left, &slab_start, &slab_end, &pages);
        if (!error)
            sizeClassDataUpdateSlabSize(*size_class_data, slab_start, slab_end, int(pages));
        else
            reportError("Invalid settings for slab_sizes");
    } while (!error && value_length_left > 0);
    return done();
}

bool ConfigurationPair::handleProfilingTimeResolution()
{
    if (!match("prof_time_resolution"))
        return false;
    if (matchValue("default"))
        options.profiling_time_resolution = ProfilingTimeResolution::Default;
    else if (matchValue("high"))
    {
        if (!config_high_resolution_timer)
            reportError("No high resolution timer support");
        else
            options.profiling_time_resolution = ProfilingTimeResolution::High;
    }
    else
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleTransparentHugePages()
{
    if (!match("thp"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < transparent_huge_pages_mode_names_limit; ++m)
    {
        if (matchValuePrefix(transparent_huge_pages_mode_names[m]))
        {
            if (!config::have_madvise_huge && !have_memcntl)
                reportError("No THP support");
            options.transparent_huge_pages = TransparentHugePagesMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleZeroRealloc()
{
    if (!match("zero_realloc"))
        return false;
    if (matchValue("alloc"))
        options.zero_realloc_action = ZeroReallocAction::Alloc;
    else if (matchValue("free"))
        options.zero_realloc_action = ZeroReallocAction::Free;
    else if (matchValue("abort"))
        options.zero_realloc_action = ZeroReallocAction::Abort;
    else
        reportError("Invalid conf value");
    return done();
}

bool ConfigurationPair::handleLog2SanitizerUseAfterFreeAlign()
{
    if (!config::use_after_free_detection || !match("lg_san_uaf_align"))
        return false;
    ssize_t a;
    /// jemalloc compatibility: on a parse error the value returned by `malloc_strtoumax` is still used below.
    if (valueRead(a) || a < -1)
        reportError("Invalid conf value");
    if (a == -1)
    {
        options.log2_sanitizer_use_after_free_align = -1;
        return done();
    }

    /// Clip if necessary.
    ssize_t max_allowed = (sizeof(size_t) << 3) - 1;
    ssize_t min_allowed = LOG2_PAGE;
    if (a > max_allowed)
        a = max_allowed;
    else if (a < min_allowed)
        a = min_allowed;

    options.log2_sanitizer_use_after_free_align = a;
    return done();
}

/// The option table, in jemalloc's matching order (the first handler whose key matches processes the pair).
void ConfigurationPair::process()
{
    if (handleBool(options.confirm_configuration, "confirm_conf"))
        return;
    if (initial_call)
        return;

    if (handleBool(options.abort, "abort") || handleBool(options.abort_configuration, "abort_conf")
        || handleBool(options.cache_oblivious, "cache_oblivious") || handleBool(options.trust_madvise, "trust_madvise")
        || handleBool(options.experimental_huge_page_allocator_start_huge, "experimental_hpa_start_huge_if_thp_always")
        || handleBool(options.experimental_huge_page_allocator_enforce_hugify, "experimental_hpa_enforce_hugify")
        || handleBool(options.huge_arena_transparent_huge_pages, "huge_arena_pac_thp") || handleMetadataTransparentHugePages()
        || handleBool(options.retain, "retain") || handleSbrk() || handleNumArenas() || handleNumArenasRatio() || handleBinShards()
        || handleThreadCacheNumCachedMax()
        || handleInteger<int64_t>(options.mutex_max_spin, "mutex_max_spin", -1, INT64_MAX, true, false, false))
        return;

    constexpr ssize_t decay_ms_max
        = NANOSECONDS_MAX_SECONDS * 1000 < uint64_t(SSIZE_MAX) ? ssize_t(NANOSECONDS_MAX_SECONDS * 1000) : SSIZE_MAX;
    if (handleSsize(options.dirty_decay_ms, "dirty_decay_ms", -1, decay_ms_max)
        || handleSsize(options.muzzy_decay_ms, "muzzy_decay_ms", -1, decay_ms_max)
        || handleInteger<size_t>(
            options.process_madvise_max_batch, "process_madvise_max_batch", 0, PROCESS_MADVISE_MAX_BATCH_LIMIT, false, true, true)
        || handleBool(options.stats_print, "stats_print") || handleStatsOptions(options.stats_print_options, "stats_print_opts")
        || handleInteger<int64_t>(options.stats_interval, "stats_interval", -1, INT64_MAX, true, false, false)
        || handleStatsOptions(options.stats_interval_options, "stats_interval_opts"))
        return;

    if constexpr (config::fill)
    {
        if (handleJunk() || handleBool(options.zero, "zero"))
            return;
    }
    /// `config_utrace` and `config_abort_on_out_of_memory` are false: `utrace` and `abort_on_out_of_memory` are invalid keys.
    if constexpr (config::enable_cxx)
    {
        if (handleBool(options.experimental_infallible_new, "experimental_infallible_new"))
            return;
    }

    if (handleBool(options.experimental_thread_cache_gc, "experimental_tcache_gc") || handleBool(options.thread_cache, "tcache")
        || handleInteger<size_t>(options.thread_cache_max, "tcache_max", 0, THREAD_CACHE_MAX_CLASS_LIMIT, false, true, true)
        || handleLog2ThreadCacheMax()
        /// Anyone trying to set a value outside -16 to 16 is deeply confused.
        || handleSsize(options.log2_thread_cache_num_slots_multiplier, "lg_tcache_nslots_mul", -16, 16)
        /// Ditto with values past 2048.
        || handleInteger<unsigned>(options.thread_cache_num_slots_small_min, "tcache_nslots_small_min", 1, 2048, true, true, true)
        || handleInteger<unsigned>(options.thread_cache_num_slots_small_max, "tcache_nslots_small_max", 1, 2048, true, true, true)
        || handleInteger<unsigned>(options.thread_cache_num_slots_large, "tcache_nslots_large", 1, 2048, true, true, true)
        || handleInteger<size_t>(options.thread_cache_gc_increment_bytes, "tcache_gc_incr_bytes", 1024, SIZE_MAX, true, false, true)
        || handleInteger<size_t>(options.thread_cache_gc_delay_bytes, "tcache_gc_delay_bytes", 0, SIZE_MAX, false, false, false)
        || handleInteger<unsigned>(options.log2_thread_cache_flush_small_division, "lg_tcache_flush_small_div", 1, 16, true, true, true)
        || handleInteger<unsigned>(options.log2_thread_cache_flush_large_division, "lg_tcache_flush_large_div", 1, 16, true, true, true)
        || handleInteger<unsigned>(options.debug_double_free_max_scan, "debug_double_free_max_scan", 0, UINT_MAX, false, false, false)
        || handleInteger<size_t>(
            options.calloc_madvise_threshold, "calloc_madvise_threshold", 0, SIZE_CLASS_LARGE_MAX_CLASS, false, true, false)
        /// The run-time option of oversize_threshold remains undocumented. It may be tweaked in the next major
        /// release (6.0). The default value 8M is rather conservative / safe. Tuning it further down may improve
        /// fragmentation a bit more, but may also cause contention on the huge arena.
        || handleInteger<size_t>(options.oversize_threshold, "oversize_threshold", 0, SIZE_CLASS_LARGE_MAX_CLASS, false, true, false)
        || handleInteger<size_t>(
            options.log2_extent_max_active_fit, "lg_extent_max_active_fit", 0, sizeof(size_t) << 3, false, true, false))
        return;

    if (handlePerCPUArena() || handleBool(options.background_thread, "background_thread")
        || handleInteger<size_t>(
            options.max_background_threads, "max_background_threads", 1, options.max_background_threads, true, true, true)
        || handleBool(options.huge_page_allocator, "hpa")
        || handleInteger<size_t>(
            options.huge_page_allocator_options.slab_max_alloc, "hpa_slab_max_alloc", PAGE, HUGE_PAGE, true, true, true)
        /// Accept either a ratio-based or an exact hugification threshold.
        || handleInteger<size_t>(
            options.huge_page_allocator_options.hugification_threshold, "hpa_hugification_threshold", PAGE, HUGE_PAGE, true, true, true)
        || handleHugePageRatio(options.huge_page_allocator_options.hugification_threshold, "hpa_hugification_threshold_ratio")
        || handleInteger<uint64_t>(options.huge_page_allocator_options.hugify_delay_ms, "hpa_hugify_delay_ms", 0, 0, false, false, false)
        || handleBool(options.huge_page_allocator_options.hugify_sync, "hpa_hugify_sync")
        || handleInteger<uint64_t>(
            options.huge_page_allocator_options.min_purge_interval_ms, "hpa_min_purge_interval_ms", 0, 0, false, false, false)
        || handleSsize(
            options.huge_page_allocator_options.experimental_max_purge_num_huge_pages, "experimental_hpa_max_purge_nhp", -1, SSIZE_MAX)
        /// Accept either a ratio-based or an exact purge threshold.
        || handleInteger<size_t>(
            options.huge_page_allocator_options.purge_threshold, "hpa_purge_threshold", PAGE, HUGE_PAGE, true, true, true)
        || handleHugePageRatio(options.huge_page_allocator_options.purge_threshold, "hpa_purge_threshold_ratio")
        || handleInteger<uint64_t>(
            options.huge_page_allocator_options.min_purge_delay_ms, "hpa_min_purge_delay_ms", 0, UINT64_MAX, false, false, false)
        || handleHugePageAllocatorHugifyStyle() || handleHugePageAllocatorDirtyMultiplier()
        || handleInteger<size_t>(options.small_extent_cache_options.num_shards, "hpa_sec_nshards", 0, 0, true, false, true)
        || handleInteger<size_t>(
            options.small_extent_cache_options.max_alloc, "hpa_sec_max_alloc", PAGE, USABLE_SIZE_GROW_SLOW_THRESHOLD, true, true, true)
        || handleInteger<size_t>(
            options.small_extent_cache_options.max_bytes, "hpa_sec_max_bytes", SMALL_EXTENT_CACHE_MAX_BYTES_DEFAULT, 0, true, false, true)
        || handleInteger<size_t>(
            options.small_extent_cache_options.batch_fill_extra, "hpa_sec_batch_fill_extra", 1, HUGE_PAGE_PAGES, true, true, true)
        || handleSlabSizes())
        return;

    if constexpr (config::profiling)
    {
        if (handleBool(options.profiling, "prof") || handleCharPtr(options.profiling_prefix, "prof_prefix")
            || handleBool(options.profiling_active, "prof_active")
            || handleBool(options.profiling_thread_active_init, "prof_thread_active_init")
            || handleInteger<size_t>(options.log2_profiling_sample, "lg_prof_sample", 0, (sizeof(uint64_t) << 3) - 1, false, true, true)
            || handleBool(options.profiling_accumulated, "prof_accum")
            || handleInteger<unsigned>(options.profiling_backtrace_max, "prof_bt_max", 1, PROFILING_BACKTRACE_MAX_LIMIT, true, true, true)
            || handleSsize(options.log2_profiling_interval, "lg_prof_interval", -1, (sizeof(uint64_t) << 3) - 1)
            || handleBool(options.profiling_growth_dump, "prof_gdump") || handleBool(options.profiling_final, "prof_final")
            || handleBool(options.profiling_leak, "prof_leak") || handleBool(options.profiling_leak_error, "prof_leak_error")
            || handleBool(options.profiling_log, "prof_log") || handleBool(options.profiling_pid_namespace, "prof_pid_namespace")
            || handleSsize(options.profiling_recent_alloc_max, "prof_recent_alloc_max", -1, SSIZE_MAX)
            || handleBool(options.profiling_stats, "prof_stats") || handleBool(options.profiling_system_thread_name, "prof_sys_thread_name")
            || handleProfilingTimeResolution()
            /// Undocumented. When set to false, don't correct for an unbiasing bug in jeprof attribution. This can be
            /// handy if you want to get consistent numbers from your binary across different jemalloc versions, even
            /// if those numbers are incorrect. The default is true.
            || handleBool(options.profiling_unbias, "prof_unbias"))
            return;
    }
    /// `config_log` is false: `log` is an invalid key.

    if (handleTransparentHugePages() || handleZeroRealloc() || handleLog2SanitizerUseAfterFreeAlign()
        || handleInteger<size_t>(options.sanitizer_guard_small, "san_guard_small", 0, SIZE_MAX, false, false, false)
        || handleInteger<size_t>(options.sanitizer_guard_large, "san_guard_large", 0, SIZE_MAX, false, false, false)
        /// Disabling large size classes is now the default behavior in jemalloc. Although it is configurable in
        /// MALLOC_CONF, this is mainly for debugging purposes and should not be tuned.
        || handleBool(options.disable_large_size_classes, "disable_large_size_classes"))
        return;

    reportError("Invalid conf pair");
}

/// jemalloc: malloc_conf_init_helper
void mallocConfInitHelper(
    SizeClassData * size_class_data, unsigned * bin_shard_sizes, bool initial_call, const char ** options_cache, char * readlink_buf)
{
    static constexpr const char * options_explain[MALLOC_CONF_NUM_SOURCES] = {
        "string specified via --with-malloc-conf",
        "string pointed to by the global variable malloc_conf",
        "\"name\" of the file referenced by the symbolic link named /etc/malloc.conf",
        "value of the environment variable MALLOC_CONF",
        "string pointed to by the global variable malloc_conf_2_conf_harder",
    };

    for (unsigned i = 0; i < MALLOC_CONF_NUM_SOURCES; ++i)
    {
        /// Get runtime configuration.
        if (initial_call)
            options_cache[i] = obtainMallocConf(i, readlink_buf);
        const char * options_string = options_cache[i];
        if (!initial_call && options.confirm_configuration)
            printMessage(
                "<jemalloc>: malloc_conf #%u (%s): \"%s\"\n", i + 1, options_explain[i], options_string != nullptr ? options_string : "");
        if (options_string == nullptr)
            continue;

        const char * k;
        const char * v;
        size_t key_length;
        size_t value_length;
        while (*options_string != '\0' && !configurationNext(&options_string, &k, &key_length, &v, &value_length))
            ConfigurationPair(initial_call, size_class_data, bin_shard_sizes, k, key_length, v, value_length).process();

        validateHugePageAllocatorSettings();
        if (options.abort_configuration && had_configuration_error)
            mallocAbortInvalidConfiguration();
    }
    /// jemalloc stores `log_init_done` here (`log.c` is dropped).
}

}

/// jemalloc: conf_next
bool configurationNext(
    const char ** options_string_ptr, const char ** key_ptr, size_t * key_length_ptr, const char ** value_ptr, size_t * value_length_ptr)
{
    const char * options_string = *options_string_ptr;

    *key_ptr = options_string;

    for (bool accept = false; !accept;)
    {
        char c = *options_string;
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
        {
            ++options_string;
        }
        else if (c == ':')
        {
            ++options_string;
            *key_length_ptr = size_t(options_string - 1 - *key_ptr);
            *value_ptr = options_string;
            accept = true;
        }
        else if (c == '\0')
        {
            if (options_string != *options_string_ptr)
            {
                mallocConfFormatError("Conf string ends with key", *options_string_ptr, options_string - 1);
                had_configuration_error = true;
            }
            return true;
        }
        else
        {
            mallocConfFormatError("Malformed conf string", *options_string_ptr, options_string);
            had_configuration_error = true;
            return true;
        }
    }

    for (bool accept = false; !accept;)
    {
        switch (*options_string)
        {
            case ',':
                ++options_string;
                /// Look ahead one character here, because the next time this function is called, it will assume that
                /// end of input has been cleanly reached if no input remains, but we have optimistically already
                /// consumed the comma if one exists.
                if (*options_string == '\0')
                {
                    mallocConfFormatError("Conf string ends with comma", *options_string_ptr, options_string - 1);
                    had_configuration_error = true;
                }
                *value_length_ptr = size_t(options_string - 1 - *value_ptr);
                accept = true;
                break;
            case '\0':
                *value_length_ptr = size_t(options_string - *value_ptr);
                accept = true;
                break;
            default: ++options_string; break;
        }
    }

    *options_string_ptr = options_string;
    return false;
}

/// jemalloc: malloc_abort_invalid_conf
void mallocAbortInvalidConfiguration()
{
    ALLOCATOR_ASSERT(options.abort_configuration);
    printMessage("<jemalloc>: Abort (abort_conf:true) on invalid conf value (see above).\n");
    /// jemalloc: invalid_conf_abort
    abort();
}

/// jemalloc: conf_error
void configurationError(const char * message, const char * k, size_t key_length, const char * v, size_t value_length)
{
    printMessage("<jemalloc>: %s: %.*s:%.*s\n", message, int(key_length), k, int(value_length), v);
    /// If abort_conf is set, error out after processing all options.
    const char * experimental = "experimental_";
    if (strncmp(k, experimental, strlen(experimental)) == 0)
    {
        /// However, tolerate experimental features.
        return;
    }
    static constexpr const char * deprecated[] = {"hpa_sec_bytes_after_flush"};
    for (const char * name : deprecated)
    {
        if (strncmp(k, name, strlen(name)) == 0)
        {
            /// Tolerate deprecated features.
            return;
        }
    }
    had_configuration_error = true;
}

/// jemalloc: conf_handle_bool
bool configurationHandleBool(const char * v, size_t value_length, bool * result)
{
    if (sizeof("true") - 1 == value_length && strncmp("true", v, value_length) == 0)
        *result = true;
    else if (sizeof("false") - 1 == value_length && strncmp("false", v, value_length) == 0)
        *result = false;
    else
        return true;
    return false;
}

/// jemalloc: conf_handle_unsigned
bool configurationHandleUnsigned(
    const char * v, size_t value_length, uintmax_t min, uintmax_t max, bool check_min, bool check_max, bool clip, uintmax_t * result)
{
    const char * end;
    errno = 0;
    uintmax_t max_value = strToUIntMax(v, &end, 0);
    if (errno != 0 || size_t(end - v) != value_length)
        return true;
    if (clip)
    {
        if (check_min && max_value < min)
            *result = min;
        else if (check_max && max_value > max)
            *result = max;
        else
            *result = max_value;
    }
    else
    {
        if ((check_min && max_value < min) || (check_max && max_value > max))
            return true;
        *result = max_value;
    }
    return false;
}

/// jemalloc: conf_handle_signed
bool configurationHandleSigned(
    const char * v, size_t value_length, intmax_t min, intmax_t max, bool check_min, bool check_max, bool clip, intmax_t * result)
{
    const char * end;
    errno = 0;
    intmax_t max_value = static_cast<intmax_t>(strToUIntMax(v, &end, 0));
    if (errno != 0 || size_t(end - v) != value_length)
        return true;
    if (clip)
    {
        if (check_min && max_value < min)
            *result = min;
        else if (check_max && max_value > max)
            *result = max;
        else
            *result = max_value;
    }
    else
    {
        if ((check_min && max_value < min) || (check_max && max_value > max))
            return true;
        *result = max_value;
    }
    return false;
}

/// jemalloc: conf_handle_char_p
bool configurationHandleCharPtr(const char * v, size_t value_length, char * destination, size_t destination_size)
{
    size_t copy_length = (value_length <= destination_size - 1) ? value_length : destination_size - 1;
    strncpy(destination, v, copy_length);
    destination[copy_length] = '\0';
    return false;
}

/// jemalloc: multi_setting_parse_next
bool multiSettingParseNext(const char ** setting_segment_current, size_t * len_left, size_t * key_start, size_t * key_end, size_t * value)
{
    const char * current = *setting_segment_current;
    const char * end;
    uintmax_t parsed;

    errno = 0;

    /// First number, then '-'.
    parsed = strToUIntMax(current, &end, 0);
    if (errno != 0 || *end != '-')
        return true;
    *key_start = size_t(parsed);
    current = end + 1;

    /// Second number, then ':'.
    parsed = strToUIntMax(current, &end, 0);
    if (errno != 0 || *end != ':')
        return true;
    *key_end = size_t(parsed);
    current = end + 1;

    /// Last number.
    parsed = strToUIntMax(current, &end, 0);
    if (errno != 0)
        return true;
    *value = size_t(parsed);

    /// Consume the separator if there is one.
    if (*end == '|')
        ++end;

    *len_left -= size_t(end - *setting_segment_current);
    *setting_segment_current = end;

    return false;
}

/// jemalloc: tcache_bin_info_default_init
bool threadCacheBinInfoDefaultInit(const char * bin_settings_segment_current, size_t len_left)
{
    return threadCacheBinInfoSettingsParse(
        bin_settings_segment_current,
        len_left,
        [](SizeClassIdx i, uint16_t num_cached_max)
        {
            /// jemalloc: cache_bin_info_init
            options.thread_cache_num_cached_max[i] = num_cached_max;
            options.thread_cache_num_cached_max_set[i] = true;
        });
}

/// jemalloc: malloc_conf_init
void mallocConfInit(SizeClassData & size_class_data, unsigned * bin_shard_sizes, char * readlink_buf)
{
    const char * options_cache[MALLOC_CONF_NUM_SOURCES] = {nullptr, nullptr, nullptr, nullptr, nullptr};

    /// The first call only sets the confirm_conf option and opts_cache.
    mallocConfInitHelper(nullptr, nullptr, true, options_cache, readlink_buf);
    mallocConfInitHelper(&size_class_data, bin_shard_sizes, false, options_cache, nullptr);
    if (mallocConfInitCheckDependencies())
    {
        /// check_deps does warning msg only; abort below if needed.
        if (options.abort_configuration)
            mallocAbortInvalidConfiguration();
    }
}

/// jemalloc: the `opt_hpa && !hpa_supported()` check in `malloc_init_hard_a0_locked`
void hugePageAllocatorDisableUnsupported()
{
    if (options.huge_page_allocator && !hugePageAllocatorSupported())
    {
        printMessage(
            "<jemalloc>: HPA not supported in the current configuration; %s.", options.abort_configuration ? "aborting" : "disabling");
        if (options.abort_configuration)
            mallocAbortInvalidConfiguration();
        else
            options.huge_page_allocator = false;
    }
}

}
