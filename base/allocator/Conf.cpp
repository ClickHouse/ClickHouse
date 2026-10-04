#include <allocator/Conf.h>

#include <allocator/ExtentHooks.h>
#include <allocator/FixedPoint.h>
#include <allocator/Format.h>
#include <allocator/NsTime.h>
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

constinit bool had_conf_error = false;

namespace
{

/// ClickHouse defines `JEMALLOC_CONFIG_ENV` but not `JEMALLOC_CONFIG_FILE` (`jemalloc-cmake/include/jemalloc/jemalloc_defs.h`),
/// so the `/etc/je_malloc.conf` symlink is never read.
constexpr bool config_file = false;

/// `JEMALLOC_HAVE_CLOCK_REALTIME` is defined on all platforms.
constexpr bool config_high_res_timer = true;

/// `JEMALLOC_HAVE_MEMCNTL` is not defined on any platform.
constexpr bool have_memcntl = false;

/// `JEMALLOC_HAVE_MADVISE_COLLAPSE` is not defined on any platform.
constexpr bool have_madvise_collapse = false;

/// jemalloc: HUGEPAGE_MAX_EXPECTED_SIZE (`pages.h`)
constexpr size_t HUGEPAGE_MAX_EXPECTED_SIZE = size_t(16) << 20;

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
void initOptStatsOpts(const char * v, size_t vlen, char * dest)
{
    size_t opts_len = strlen(dest);
    JE_ASSERT(opts_len <= stats_print_tot_num_options);

    for (size_t i = 0; i < vlen; ++i)
    {
        if (v[i] == '\0' || strchr(stats_print_option_chars, v[i]) == nullptr)
            continue;

        if (strchr(dest, v[i]) != nullptr)
        {
            /// Ignore repeated.
            continue;
        }

        dest[opts_len++] = v[i];
        dest[opts_len] = '\0';
        JE_ASSERT(opts_len <= stats_print_tot_num_options);
    }
    JE_ASSERT(opts_len == strlen(dest));
}

/// jemalloc: malloc_conf_format_error
void mallocConfFormatError(const char * msg, const char * begin, const char * end)
{
    size_t len = size_t(end - begin + 1);
    len = len > BUFERROR_BUF ? BUFERROR_BUF : len;

    printMessage("<jemalloc>: %s -- %.*s\n", msg, int(len), begin);
}

/// jemalloc: obtain_malloc_conf
const char * obtainMallocConf(unsigned which_source, char * readlink_buf)
{
    JE_ASSERT(which_source < MALLOC_CONF_NSOURCES);

    const char * ret;
    switch (which_source)
    {
        case 0:
            ret = config::malloc_conf_default;
            break;
        case 1:
            if (je_malloc_conf != nullptr)
            {
                /// Use options that were compiled into the program.
                ret = je_malloc_conf;
            }
            else
            {
                /// No configuration specified.
                ret = nullptr;
            }
            break;
        case 2:
        {
            if constexpr (!config_file)
            {
                ret = nullptr;
                break;
            }
            else
            {
                ssize_t linklen = 0;
                int saved_errno = errno;
                const char * linkname = "/etc/je_malloc.conf";

                /// Try to use the contents of the "/etc/malloc.conf" symbolic link's name.
                linklen = readlink(linkname, readlink_buf, PATH_MAX);
                if (linklen == -1)
                {
                    /// No configuration specified.
                    linklen = 0;
                    /// Restore errno.
                    errno = saved_errno;
                }
                readlink_buf[linklen] = '\0';
                ret = readlink_buf;
                break;
            }
        }
        case 3:
        {
            const char * envname = "MALLOC_CONF";
            if ((ret = jemallocGetenv(envname)) != nullptr)
                opt.malloc_conf_env_var = ret;
            else
            {
                /// No configuration specified.
                ret = nullptr;
            }
            break;
        }
        case 4:
            ret = je_malloc_conf_2_conf_harder;
            break;
        default:
            JE_NOT_REACHED();
    }
    return ret;
}

/// jemalloc: validate_hpa_settings
void validateHpaSettings()
{
    if (!hpaSupported() || !opt.hpa)
        return;
    if (HUGEPAGE > HUGEPAGE_MAX_EXPECTED_SIZE)
    {
        had_conf_error = true;
        printMessage("<jemalloc>: huge page size (%zu) greater than expected.May not be supported or behave as expected.", HUGEPAGE);
    }
    if (!have_madvise_collapse && opt.hpa_opts.hugify_sync)
    {
        had_conf_error = true;
        printMessage(
            "<jemalloc>: hpa_hugify_sync config option is enabled, but MADV_COLLAPSE support was not detected at build time.");
    }
}

/// jemalloc: malloc_conf_init_check_deps. Returns true if there is an inconsistency.
bool mallocConfInitCheckDeps()
{
    if (opt.prof_leak_error && !opt.prof_final)
    {
        printMessage("<jemalloc>: prof_leak_error is set w/o prof_final.\n");
        return true;
    }
    /// To emphasize in the stats output that opt is disabled when !debug (jemalloc's `config_debug` is always false).
    opt.debug_double_free_max_scan = 0;

    return false;
}

/// The handling of one `key:value` pair: the body of the parsing loop of `malloc_conf_init_helper` with its macros
/// (`CONF_MATCH`, `CONF_ERROR`, `CONF_CONTINUE`, `CONF_HANDLE_*`) as methods. Every `handle*` method returns true if
/// the key matched (the pair is then fully processed: `CONF_CONTINUE`).
class ConfPair
{
public:
    ConfPair(bool initial_call_, SizeClassData * sc_data_, unsigned * bin_shard_sizes_, const char * k_, size_t klen_, const char * v_, size_t vlen_)
        : initial_call(initial_call_)
        , sc_data(sc_data_)
        , bin_shard_sizes(bin_shard_sizes_)
        , k(k_)
        , klen(klen_)
        , v(v_)
        , vlen(vlen_)
    {
    }

    void process();

private:
    const bool initial_call;
    SizeClassData * const sc_data;
    unsigned * const bin_shard_sizes;
    const char * const k;
    const size_t klen;
    const char * const v;
    const size_t vlen;
    bool cur_opt_valid = true;

    /// jemalloc: CONF_MATCH
    bool match(const char * n) const { return strlen(n) == klen && strncmp(n, k, klen) == 0; }

    /// jemalloc: CONF_MATCH_VALUE
    bool matchValue(const char * n) const { return strlen(n) == vlen && strncmp(n, v, vlen) == 0; }

    /// The keys `metadata_thp`, `dss`, `percpu_arena`, `hpa_hugify_style` are compared with `strncmp(n, k, klen)`, so
    /// any prefix of the name matches (including the empty key).
    bool matchKeyPrefix(const char * n) const { return strncmp(n, k, klen) == 0; }

    /// Values of enum options are compared with `strncmp(name, v, vlen)`: any prefix of the name matches.
    bool matchValuePrefix(const char * name) const { return strncmp(name, v, vlen) == 0; }

    /// jemalloc: CONF_ERROR
    void error(const char * msg)
    {
        if (!initial_call)
        {
            confError(msg, k, klen, v, vlen);
            cur_opt_valid = false;
        }
    }

    /// jemalloc: CONF_CONTINUE (without the `continue`)
    bool done() const
    {
        if (!initial_call && opt.confirm_conf && cur_opt_valid)
            printMessage("<jemalloc>: -- Set conf value: %.*s:%.*s\n", int(klen), k, int(vlen), v);
        return true;
    }

    /// jemalloc: CONF_HANDLE_BOOL
    bool handleBool(bool & o, const char * n)
    {
        if (!match(n))
            return false;
        if (confHandleBool(v, vlen, &o))
            error("Invalid conf value");
        return done();
    }

    /// jemalloc: CONF_VALUE_READ, CONF_VALUE_READ_FAIL. Returns true on failure.
    template <typename MaxT>
    bool valueRead(MaxT & result) const
    {
        const char * end;
        errno = 0;
        result = static_cast<MaxT>(strToUMax(v, &end, 0));
        return errno != 0 || size_t(end - v) != vlen;
    }

    /// jemalloc: CONF_HANDLE_T with `max_t` = `intmax_t` for signed `T`, `uintmax_t` otherwise
    /// (`CONF_HANDLE_UNSIGNED`, `CONF_HANDLE_SIZE_T`, `CONF_HANDLE_INT64_T`, `CONF_HANDLE_UINT64_T`).
    template <typename T>
    bool handleT(T & o, const char * n, T min, T max, bool check_min, bool check_max, bool clip)
    {
        using MaxT = std::conditional_t<std::is_signed_v<T>, intmax_t, uintmax_t>;
        if (!match(n))
            return false;
        MaxT mv;
        if (valueRead(mv))
            error("Invalid conf value");
        else if (clip)
        {
            if (check_min && mv < MaxT(min))
                o = min;
            else if (check_max && mv > MaxT(max))
                o = max;
            else
                o = static_cast<T>(mv);
        }
        else
        {
            if ((check_min && mv < MaxT(min)) || (check_max && mv > MaxT(max)))
                error("Out-of-range conf value");
            else
                o = static_cast<T>(mv);
        }
        return done();
    }

    /// jemalloc: CONF_HANDLE_SSIZE_T
    bool handleSsize(ssize_t & o, const char * n, ssize_t min, ssize_t max) { return handleT<ssize_t>(o, n, min, max, true, true, false); }

    /// jemalloc: CONF_HANDLE_CHAR_P
    template <size_t N>
    bool handleCharP(char (&o)[N], const char * n)
    {
        if (!match(n))
            return false;
        size_t cpylen = (vlen <= N - 1) ? vlen : N - 1;
        strncpy(o, v, cpylen);
        o[cpylen] = '\0';
        return done();
    }

    /// `hpa_hugification_threshold_ratio`, `hpa_purge_threshold_ratio`.
    bool handleHugepageRatio(size_t & o, const char * n)
    {
        if (!match(n))
            return false;
        FixedPoint ratio;
        const char * end;
        bool err = fxp::parse(&ratio, v, &end);
        if (err || size_t(end - v) != vlen || ratio > fxp::initInt(1))
            error("Invalid conf value");
        else
            o = fxp::mulFrac(HUGEPAGE, ratio);
        return done();
    }

    bool handleMetadataThp();
    bool handleDss();
    bool handleNarenas();
    bool handleNarenasRatio();
    bool handleBinShards();
    bool handleTcacheNcachedMax();
    bool handleStatsOpts(char * dest, const char * n);
    bool handleJunk();
    bool handleLgTcacheMax();
    bool handlePercpuArena();
    bool handleHpaHugifyStyle();
    bool handleHpaDirtyMult();
    bool handleSlabSizes();
    bool handleProfTimeResolution();
    bool handleThp();
    bool handleZeroRealloc();
    bool handleLgSanUafAlign();
};

bool ConfPair::handleMetadataThp()
{
    if (!matchKeyPrefix("metadata_thp"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < metadata_thp_mode_limit; ++m)
    {
        if (matchValuePrefix(metadata_thp_mode_names[m]))
        {
            opt.metadata_thp = MetadataThpMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleDss()
{
    if (!matchKeyPrefix("dss"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < unsigned(DssPrec::Limit); ++m)
    {
        if (matchValuePrefix(dss_prec_names[m]))
        {
            if (extentDssPrecSet(DssPrec(m)))
                error("Error setting dss");
            else
            {
                opt.dss = dss_prec_names[m];
                found = true;
                break;
            }
        }
    }
    if (!found)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleNarenas()
{
    if (!match("narenas"))
        return false;
    if (matchValue("default"))
    {
        opt.narenas = 0;
        return done();
    }
    return handleT<unsigned>(opt.narenas, "narenas", 1, UINT_MAX, true, false, false);
}

bool ConfPair::handleNarenasRatio()
{
    if (!match("narenas_ratio"))
        return false;
    const char * end;
    bool err = fxp::parse(&opt.narenas_ratio, v, &end);
    if (err || size_t(end - v) != vlen)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleBinShards()
{
    if (!match("bin_shards"))
        return false;
    const char * bin_shards_segment_cur = v;
    size_t vlen_left = vlen;
    do
    {
        size_t size_start;
        size_t size_end;
        size_t nshards;
        bool err = multiSettingParseNext(&bin_shards_segment_cur, &vlen_left, &size_start, &size_end, &nshards);
        if (err || binUpdateShardSize(bin_shard_sizes, size_start, size_end, nshards))
        {
            error("Invalid settings for bin_shards");
            break;
        }
    } while (vlen_left > 0);
    return done();
}

bool ConfPair::handleTcacheNcachedMax()
{
    if (!match("tcache_ncached_max"))
        return false;
    if (tcacheBinInfoDefaultInit(v, vlen))
        error("Invalid settings for tcache_ncached_max");
    return done();
}

bool ConfPair::handleStatsOpts(char * dest, const char * n)
{
    if (!match(n))
        return false;
    initOptStatsOpts(v, vlen, dest);
    return done();
}

bool ConfPair::handleJunk()
{
    if (!match("junk"))
        return false;
    if (matchValue("true"))
    {
        opt.junk = JUNK_TRUE;
        opt.junk_alloc = opt.junk_free = true;
    }
    else if (matchValue("false"))
    {
        opt.junk = JUNK_FALSE;
        opt.junk_alloc = opt.junk_free = false;
    }
    else if (matchValue("alloc"))
    {
        opt.junk = JUNK_ALLOC;
        opt.junk_alloc = true;
        opt.junk_free = false;
    }
    else if (matchValue("free"))
    {
        opt.junk = JUNK_FREE;
        opt.junk_alloc = false;
        opt.junk_free = true;
    }
    else
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleLgTcacheMax()
{
    if (!match("lg_tcache_max"))
        return false;
    size_t m;
    if (valueRead(m))
        error("Invalid conf value");
    else
    {
        /// Clip if necessary.
        if (m > TCACHE_LG_MAXCLASS_LIMIT)
            m = TCACHE_LG_MAXCLASS_LIMIT;
        opt.tcache_max = size_t(1) << m;
    }
    return done();
}

bool ConfPair::handlePercpuArena()
{
    if (!matchKeyPrefix("percpu_arena"))
        return false;
    bool found = false;
    for (unsigned m = percpu_arena_mode_names_base; m < percpu_arena_mode_names_limit; ++m)
    {
        if (matchValuePrefix(percpu_arena_mode_names[m]))
        {
            if (!config::have_percpu_arena)
                error("No getcpu support");
            opt.percpu_arena = PercpuArenaMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleHpaHugifyStyle()
{
    if (!matchKeyPrefix("hpa_hugify_style"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < hpa_hugify_style_limit; ++m)
    {
        if (matchValuePrefix(hpa_hugify_style_names[m]))
        {
            opt.hpa_opts.hugify_style = HpaHugifyStyle(m);
            found = true;
            break;
        }
    }
    if (!found)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleHpaDirtyMult()
{
    if (!match("hpa_dirty_mult"))
        return false;
    if (matchValue("-1"))
    {
        opt.hpa_opts.dirty_mult = FixedPoint(-1);
        return done();
    }
    FixedPoint ratio;
    const char * end;
    bool err = fxp::parse(&ratio, v, &end);
    if (err || size_t(end - v) != vlen)
        error("Invalid conf value");
    else
        opt.hpa_opts.dirty_mult = ratio;
    return done();
}

bool ConfPair::handleSlabSizes()
{
    if (!match("slab_sizes"))
        return false;
    if (matchValue("default"))
    {
        scDataInit(*sc_data);
        return done();
    }
    bool err;
    const char * slab_size_segment_cur = v;
    size_t vlen_left = vlen;
    do
    {
        size_t slab_start;
        size_t slab_end;
        size_t pgs;
        err = multiSettingParseNext(&slab_size_segment_cur, &vlen_left, &slab_start, &slab_end, &pgs);
        if (!err)
            scDataUpdateSlabSize(*sc_data, slab_start, slab_end, int(pgs));
        else
            error("Invalid settings for slab_sizes");
    } while (!err && vlen_left > 0);
    return done();
}

bool ConfPair::handleProfTimeResolution()
{
    if (!match("prof_time_resolution"))
        return false;
    if (matchValue("default"))
        opt.prof_time_res = ProfTimeRes::Default;
    else if (matchValue("high"))
    {
        if (!config_high_res_timer)
            error("No high resolution timer support");
        else
            opt.prof_time_res = ProfTimeRes::High;
    }
    else
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleThp()
{
    if (!match("thp"))
        return false;
    bool found = false;
    for (unsigned m = 0; m < thp_mode_names_limit; ++m)
    {
        if (matchValuePrefix(thp_mode_names[m]))
        {
            if (!config::have_madvise_huge && !have_memcntl)
                error("No THP support");
            opt.thp = ThpMode(m);
            found = true;
            break;
        }
    }
    if (!found)
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleZeroRealloc()
{
    if (!match("zero_realloc"))
        return false;
    if (matchValue("alloc"))
        opt.zero_realloc_action = ZeroReallocAction::Alloc;
    else if (matchValue("free"))
        opt.zero_realloc_action = ZeroReallocAction::Free;
    else if (matchValue("abort"))
        opt.zero_realloc_action = ZeroReallocAction::Abort;
    else
        error("Invalid conf value");
    return done();
}

bool ConfPair::handleLgSanUafAlign()
{
    if (!config::uaf_detection || !match("lg_san_uaf_align"))
        return false;
    ssize_t a;
    /// jemalloc compatibility: on a parse error the value returned by `malloc_strtoumax` is still used below.
    if (valueRead(a) || a < -1)
        error("Invalid conf value");
    if (a == -1)
    {
        opt.lg_san_uaf_align = -1;
        return done();
    }

    /// Clip if necessary.
    ssize_t max_allowed = (sizeof(size_t) << 3) - 1;
    ssize_t min_allowed = LG_PAGE;
    if (a > max_allowed)
        a = max_allowed;
    else if (a < min_allowed)
        a = min_allowed;

    opt.lg_san_uaf_align = a;
    return done();
}

/// The option table, in jemalloc's matching order (the first handler whose key matches processes the pair).
void ConfPair::process()
{
    if (handleBool(opt.confirm_conf, "confirm_conf"))
        return;
    if (initial_call)
        return;

    if (handleBool(opt.abort, "abort") || handleBool(opt.abort_conf, "abort_conf")
        || handleBool(opt.cache_oblivious, "cache_oblivious") || handleBool(opt.trust_madvise, "trust_madvise")
        || handleBool(opt.experimental_hpa_start_huge_if_thp_always, "experimental_hpa_start_huge_if_thp_always")
        || handleBool(opt.experimental_hpa_enforce_hugify, "experimental_hpa_enforce_hugify")
        || handleBool(opt.huge_arena_pac_thp, "huge_arena_pac_thp") || handleMetadataThp() || handleBool(opt.retain, "retain")
        || handleDss() || handleNarenas() || handleNarenasRatio() || handleBinShards() || handleTcacheNcachedMax()
        || handleT<int64_t>(opt.mutex_max_spin, "mutex_max_spin", -1, INT64_MAX, true, false, false))
        return;

    constexpr ssize_t decay_ms_max
        = NSTIME_SEC_MAX * 1000 < uint64_t(SSIZE_MAX) ? ssize_t(NSTIME_SEC_MAX * 1000) : SSIZE_MAX;
    if (handleSsize(opt.dirty_decay_ms, "dirty_decay_ms", -1, decay_ms_max)
        || handleSsize(opt.muzzy_decay_ms, "muzzy_decay_ms", -1, decay_ms_max)
        || handleT<size_t>(
            opt.process_madvise_max_batch, "process_madvise_max_batch", 0, PROCESS_MADVISE_MAX_BATCH_LIMIT, false, true, true)
        || handleBool(opt.stats_print, "stats_print") || handleStatsOpts(opt.stats_print_opts, "stats_print_opts")
        || handleT<int64_t>(opt.stats_interval, "stats_interval", -1, INT64_MAX, true, false, false)
        || handleStatsOpts(opt.stats_interval_opts, "stats_interval_opts"))
        return;

    if constexpr (config::fill)
    {
        if (handleJunk() || handleBool(opt.zero, "zero"))
            return;
    }
    /// `config_utrace` and `config_xmalloc` are false: `utrace` and `xmalloc` are invalid keys.
    if constexpr (config::enable_cxx)
    {
        if (handleBool(opt.experimental_infallible_new, "experimental_infallible_new"))
            return;
    }

    if (handleBool(opt.experimental_tcache_gc, "experimental_tcache_gc") || handleBool(opt.tcache, "tcache")
        || handleT<size_t>(opt.tcache_max, "tcache_max", 0, TCACHE_MAXCLASS_LIMIT, false, true, true) || handleLgTcacheMax()
        /// Anyone trying to set a value outside -16 to 16 is deeply confused.
        || handleSsize(opt.lg_tcache_nslots_mul, "lg_tcache_nslots_mul", -16, 16)
        /// Ditto with values past 2048.
        || handleT<unsigned>(opt.tcache_nslots_small_min, "tcache_nslots_small_min", 1, 2048, true, true, true)
        || handleT<unsigned>(opt.tcache_nslots_small_max, "tcache_nslots_small_max", 1, 2048, true, true, true)
        || handleT<unsigned>(opt.tcache_nslots_large, "tcache_nslots_large", 1, 2048, true, true, true)
        || handleT<size_t>(opt.tcache_gc_incr_bytes, "tcache_gc_incr_bytes", 1024, SIZE_MAX, true, false, true)
        || handleT<size_t>(opt.tcache_gc_delay_bytes, "tcache_gc_delay_bytes", 0, SIZE_MAX, false, false, false)
        || handleT<unsigned>(opt.lg_tcache_flush_small_div, "lg_tcache_flush_small_div", 1, 16, true, true, true)
        || handleT<unsigned>(opt.lg_tcache_flush_large_div, "lg_tcache_flush_large_div", 1, 16, true, true, true)
        || handleT<unsigned>(opt.debug_double_free_max_scan, "debug_double_free_max_scan", 0, UINT_MAX, false, false, false)
        || handleT<size_t>(opt.calloc_madvise_threshold, "calloc_madvise_threshold", 0, SC_LARGE_MAXCLASS, false, true, false)
        /// The run-time option of oversize_threshold remains undocumented. It may be tweaked in the next major
        /// release (6.0). The default value 8M is rather conservative / safe. Tuning it further down may improve
        /// fragmentation a bit more, but may also cause contention on the huge arena.
        || handleT<size_t>(opt.oversize_threshold, "oversize_threshold", 0, SC_LARGE_MAXCLASS, false, true, false)
        || handleT<size_t>(opt.lg_extent_max_active_fit, "lg_extent_max_active_fit", 0, sizeof(size_t) << 3, false, true, false))
        return;

    if (handlePercpuArena() || handleBool(opt.background_thread, "background_thread")
        || handleT<size_t>(opt.max_background_threads, "max_background_threads", 1, opt.max_background_threads, true, true, true)
        || handleBool(opt.hpa, "hpa")
        || handleT<size_t>(opt.hpa_opts.slab_max_alloc, "hpa_slab_max_alloc", PAGE, HUGEPAGE, true, true, true)
        /// Accept either a ratio-based or an exact hugification threshold.
        || handleT<size_t>(opt.hpa_opts.hugification_threshold, "hpa_hugification_threshold", PAGE, HUGEPAGE, true, true, true)
        || handleHugepageRatio(opt.hpa_opts.hugification_threshold, "hpa_hugification_threshold_ratio")
        || handleT<uint64_t>(opt.hpa_opts.hugify_delay_ms, "hpa_hugify_delay_ms", 0, 0, false, false, false)
        || handleBool(opt.hpa_opts.hugify_sync, "hpa_hugify_sync")
        || handleT<uint64_t>(opt.hpa_opts.min_purge_interval_ms, "hpa_min_purge_interval_ms", 0, 0, false, false, false)
        || handleSsize(opt.hpa_opts.experimental_max_purge_nhp, "experimental_hpa_max_purge_nhp", -1, SSIZE_MAX)
        /// Accept either a ratio-based or an exact purge threshold.
        || handleT<size_t>(opt.hpa_opts.purge_threshold, "hpa_purge_threshold", PAGE, HUGEPAGE, true, true, true)
        || handleHugepageRatio(opt.hpa_opts.purge_threshold, "hpa_purge_threshold_ratio")
        || handleT<uint64_t>(opt.hpa_opts.min_purge_delay_ms, "hpa_min_purge_delay_ms", 0, UINT64_MAX, false, false, false)
        || handleHpaHugifyStyle() || handleHpaDirtyMult()
        || handleT<size_t>(opt.hpa_sec_opts.nshards, "hpa_sec_nshards", 0, 0, true, false, true)
        || handleT<size_t>(opt.hpa_sec_opts.max_alloc, "hpa_sec_max_alloc", PAGE, USIZE_GROW_SLOW_THRESHOLD, true, true, true)
        || handleT<size_t>(opt.hpa_sec_opts.max_bytes, "hpa_sec_max_bytes", SEC_OPTS_MAX_BYTES_DEFAULT, 0, true, false, true)
        || handleT<size_t>(opt.hpa_sec_opts.batch_fill_extra, "hpa_sec_batch_fill_extra", 1, HUGEPAGE_PAGES, true, true, true)
        || handleSlabSizes())
        return;

    if constexpr (config::prof)
    {
        if (handleBool(opt.prof, "prof") || handleCharP(opt.prof_prefix, "prof_prefix")
            || handleBool(opt.prof_active, "prof_active") || handleBool(opt.prof_thread_active_init, "prof_thread_active_init")
            || handleT<size_t>(opt.lg_prof_sample, "lg_prof_sample", 0, (sizeof(uint64_t) << 3) - 1, false, true, true)
            || handleBool(opt.prof_accum, "prof_accum")
            || handleT<unsigned>(opt.prof_bt_max, "prof_bt_max", 1, PROF_BT_MAX_LIMIT, true, true, true)
            || handleSsize(opt.lg_prof_interval, "lg_prof_interval", -1, (sizeof(uint64_t) << 3) - 1)
            || handleBool(opt.prof_gdump, "prof_gdump") || handleBool(opt.prof_final, "prof_final")
            || handleBool(opt.prof_leak, "prof_leak") || handleBool(opt.prof_leak_error, "prof_leak_error")
            || handleBool(opt.prof_log, "prof_log") || handleBool(opt.prof_pid_namespace, "prof_pid_namespace")
            || handleSsize(opt.prof_recent_alloc_max, "prof_recent_alloc_max", -1, SSIZE_MAX)
            || handleBool(opt.prof_stats, "prof_stats") || handleBool(opt.prof_sys_thread_name, "prof_sys_thread_name")
            || handleProfTimeResolution()
            /// Undocumented. When set to false, don't correct for an unbiasing bug in jeprof attribution. This can be
            /// handy if you want to get consistent numbers from your binary across different jemalloc versions, even
            /// if those numbers are incorrect. The default is true.
            || handleBool(opt.prof_unbias, "prof_unbias"))
            return;
    }
    /// `config_log` is false: `log` is an invalid key.

    if (handleThp() || handleZeroRealloc() || handleLgSanUafAlign()
        || handleT<size_t>(opt.san_guard_small, "san_guard_small", 0, SIZE_MAX, false, false, false)
        || handleT<size_t>(opt.san_guard_large, "san_guard_large", 0, SIZE_MAX, false, false, false)
        /// Disabling large size classes is now the default behavior in jemalloc. Although it is configurable in
        /// MALLOC_CONF, this is mainly for debugging purposes and should not be tuned.
        || handleBool(opt.disable_large_size_classes, "disable_large_size_classes"))
        return;

    error("Invalid conf pair");
}

/// jemalloc: malloc_conf_init_helper
void mallocConfInitHelper(
    SizeClassData * sc_data, unsigned * bin_shard_sizes, bool initial_call, const char ** opts_cache, char * readlink_buf)
{
    static constexpr const char * opts_explain[MALLOC_CONF_NSOURCES] = {
        "string specified via --with-malloc-conf",
        "string pointed to by the global variable malloc_conf",
        "\"name\" of the file referenced by the symbolic link named /etc/malloc.conf",
        "value of the environment variable MALLOC_CONF",
        "string pointed to by the global variable malloc_conf_2_conf_harder",
    };

    for (unsigned i = 0; i < MALLOC_CONF_NSOURCES; ++i)
    {
        /// Get runtime configuration.
        if (initial_call)
            opts_cache[i] = obtainMallocConf(i, readlink_buf);
        const char * opts = opts_cache[i];
        if (!initial_call && opt.confirm_conf)
            printMessage("<jemalloc>: malloc_conf #%u (%s): \"%s\"\n", i + 1, opts_explain[i], opts != nullptr ? opts : "");
        if (opts == nullptr)
            continue;

        const char * k;
        const char * v;
        size_t klen;
        size_t vlen;
        while (*opts != '\0' && !confNext(&opts, &k, &klen, &v, &vlen))
            ConfPair(initial_call, sc_data, bin_shard_sizes, k, klen, v, vlen).process();

        validateHpaSettings();
        if (opt.abort_conf && had_conf_error)
            mallocAbortInvalidConf();
    }
    /// jemalloc stores `log_init_done` here (`log.c` is dropped).
}

}

/// jemalloc: conf_next
bool confNext(const char ** opts_p, const char ** k_p, size_t * klen_p, const char ** v_p, size_t * vlen_p)
{
    const char * opts = *opts_p;

    *k_p = opts;

    for (bool accept = false; !accept;)
    {
        char c = *opts;
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
        {
            ++opts;
        }
        else if (c == ':')
        {
            ++opts;
            *klen_p = size_t(opts - 1 - *k_p);
            *v_p = opts;
            accept = true;
        }
        else if (c == '\0')
        {
            if (opts != *opts_p)
            {
                mallocConfFormatError("Conf string ends with key", *opts_p, opts - 1);
                had_conf_error = true;
            }
            return true;
        }
        else
        {
            mallocConfFormatError("Malformed conf string", *opts_p, opts);
            had_conf_error = true;
            return true;
        }
    }

    for (bool accept = false; !accept;)
    {
        switch (*opts)
        {
            case ',':
                ++opts;
                /// Look ahead one character here, because the next time this function is called, it will assume that
                /// end of input has been cleanly reached if no input remains, but we have optimistically already
                /// consumed the comma if one exists.
                if (*opts == '\0')
                {
                    mallocConfFormatError("Conf string ends with comma", *opts_p, opts - 1);
                    had_conf_error = true;
                }
                *vlen_p = size_t(opts - 1 - *v_p);
                accept = true;
                break;
            case '\0':
                *vlen_p = size_t(opts - *v_p);
                accept = true;
                break;
            default:
                ++opts;
                break;
        }
    }

    *opts_p = opts;
    return false;
}

/// jemalloc: malloc_abort_invalid_conf
void mallocAbortInvalidConf()
{
    JE_ASSERT(opt.abort_conf);
    printMessage("<jemalloc>: Abort (abort_conf:true) on invalid conf value (see above).\n");
    /// jemalloc: invalid_conf_abort
    abort();
}

/// jemalloc: conf_error
void confError(const char * msg, const char * k, size_t klen, const char * v, size_t vlen)
{
    printMessage("<jemalloc>: %s: %.*s:%.*s\n", msg, int(klen), k, int(vlen), v);
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
    had_conf_error = true;
}

/// jemalloc: conf_handle_bool
bool confHandleBool(const char * v, size_t vlen, bool * result)
{
    if (sizeof("true") - 1 == vlen && strncmp("true", v, vlen) == 0)
        *result = true;
    else if (sizeof("false") - 1 == vlen && strncmp("false", v, vlen) == 0)
        *result = false;
    else
        return true;
    return false;
}

/// jemalloc: conf_handle_unsigned
bool confHandleUnsigned(
    const char * v, size_t vlen, uintmax_t min, uintmax_t max, bool check_min, bool check_max, bool clip, uintmax_t * result)
{
    const char * end;
    errno = 0;
    uintmax_t mv = strToUMax(v, &end, 0);
    if (errno != 0 || size_t(end - v) != vlen)
        return true;
    if (clip)
    {
        if (check_min && mv < min)
            *result = min;
        else if (check_max && mv > max)
            *result = max;
        else
            *result = mv;
    }
    else
    {
        if ((check_min && mv < min) || (check_max && mv > max))
            return true;
        *result = mv;
    }
    return false;
}

/// jemalloc: conf_handle_signed
bool confHandleSigned(
    const char * v, size_t vlen, intmax_t min, intmax_t max, bool check_min, bool check_max, bool clip, intmax_t * result)
{
    const char * end;
    errno = 0;
    intmax_t mv = static_cast<intmax_t>(strToUMax(v, &end, 0));
    if (errno != 0 || size_t(end - v) != vlen)
        return true;
    if (clip)
    {
        if (check_min && mv < min)
            *result = min;
        else if (check_max && mv > max)
            *result = max;
        else
            *result = mv;
    }
    else
    {
        if ((check_min && mv < min) || (check_max && mv > max))
            return true;
        *result = mv;
    }
    return false;
}

/// jemalloc: conf_handle_char_p
bool confHandleCharP(const char * v, size_t vlen, char * dest, size_t dest_sz)
{
    size_t cpylen = (vlen <= dest_sz - 1) ? vlen : dest_sz - 1;
    strncpy(dest, v, cpylen);
    dest[cpylen] = '\0';
    return false;
}

/// jemalloc: multi_setting_parse_next
bool multiSettingParseNext(const char ** setting_segment_cur, size_t * len_left, size_t * key_start, size_t * key_end, size_t * value)
{
    const char * cur = *setting_segment_cur;
    const char * end;
    uintmax_t um;

    errno = 0;

    /// First number, then '-'.
    um = strToUMax(cur, &end, 0);
    if (errno != 0 || *end != '-')
        return true;
    *key_start = size_t(um);
    cur = end + 1;

    /// Second number, then ':'.
    um = strToUMax(cur, &end, 0);
    if (errno != 0 || *end != ':')
        return true;
    *key_end = size_t(um);
    cur = end + 1;

    /// Last number.
    um = strToUMax(cur, &end, 0);
    if (errno != 0)
        return true;
    *value = size_t(um);

    /// Consume the separator if there is one.
    if (*end == '|')
        ++end;

    *len_left -= size_t(end - *setting_segment_cur);
    *setting_segment_cur = end;

    return false;
}

/// jemalloc: tcache_bin_info_default_init
bool tcacheBinInfoDefaultInit(const char * bin_settings_segment_cur, size_t len_left)
{
    return tcacheBinInfoSettingsParse(
        bin_settings_segment_cur,
        len_left,
        [](szind_t i, uint16_t ncached_max)
        {
            /// jemalloc: cache_bin_info_init
            opt.tcache_ncached_max[i] = ncached_max;
            opt.tcache_ncached_max_set[i] = true;
        });
}

/// jemalloc: malloc_conf_init
void mallocConfInit(SizeClassData & sc_data, unsigned * bin_shard_sizes, char * readlink_buf)
{
    const char * opts_cache[MALLOC_CONF_NSOURCES] = {nullptr, nullptr, nullptr, nullptr, nullptr};

    /// The first call only sets the confirm_conf option and opts_cache.
    mallocConfInitHelper(nullptr, nullptr, true, opts_cache, readlink_buf);
    mallocConfInitHelper(&sc_data, bin_shard_sizes, false, opts_cache, nullptr);
    if (mallocConfInitCheckDeps())
    {
        /// check_deps does warning msg only; abort below if needed.
        if (opt.abort_conf)
            mallocAbortInvalidConf();
    }
}

/// jemalloc: the `opt_hpa && !hpa_supported()` check in `malloc_init_hard_a0_locked`
void hpaDisableUnsupported()
{
    if (opt.hpa && !hpaSupported())
    {
        printMessage("<jemalloc>: HPA not supported in the current configuration; %s.", opt.abort_conf ? "aborting" : "disabling");
        if (opt.abort_conf)
            mallocAbortInvalidConf();
        else
            opt.hpa = false;
    }
}

}
