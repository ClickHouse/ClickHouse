/// Tests of the configuration parser and the option defaults.
/// Ports of jemalloc's `test/unit/conf.c`, `test/unit/conf_parse.c`, `test/unit/malloc_conf_2.c`, plus pinned
/// behavior of the option table (values from jemalloc; the exhaustive comparison is in conf_oracle.cpp).

#include <allocator/Conf.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Format.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <climits>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace jemalloc;

namespace
{

/// Messages printed by the parser.
char messages[1 << 16];
size_t messages_len = 0;

void captureMessage(void *, const char * s)
{
    size_t n = strlen(s);
    REQUIRE(messages_len + n < sizeof(messages));
    memcpy(messages + messages_len, s, n + 1);
    messages_len += n;
}

void resetState()
{
    opt = Options{};
    had_conf_error = false;
    extentDssPrecSet(DSS_PREC_DEFAULT);
    messages_len = 0;
    messages[0] = '\0';
    je_malloc_message = captureMessage;
    je_malloc_conf = nullptr;
    je_malloc_conf_2_conf_harder = nullptr;
    unsetenv("MALLOC_CONF");
}

struct Boot
{
    SizeClassData sc_data{};
    unsigned bin_shard_sizes[SC_NBINS];
    char readlink_buf[MALLOC_CONF_READLINK_BUF_SIZE];

    /// The boot steps that precede `malloc_conf_init` and the parser itself.
    void run()
    {
        scBoot(sc_data);
        binShardSizesBoot(bin_shard_sizes);
        readlink_buf[0] = '\0';
        mallocConfInit(sc_data, bin_shard_sizes, readlink_buf);
    }
};

/// Parse with `MALLOC_CONF=env` (on top of the compiled-in string).
void parseEnv(const char * env, Boot & boot)
{
    resetState();
    setenv("MALLOC_CONF", env, 1);
    boot.run();
    unsetenv("MALLOC_CONF");
}

void parseEnv(const char * env)
{
    Boot boot;
    parseEnv(env, boot);
}

/// Runs `f` in a child process; returns the termination signal (0 if it exited normally).
template <typename F>
int runInChild(F && f)
{
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        f();
        _exit(0);
    }
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    return WIFSIGNALED(status) ? WTERMSIG(status) : 0;
}

}

/// --- test/unit/conf.c ----------------------------------------------------------------------------------------------

TEST(ConfNext, Simple)
{
    resetState();
    const char * opts = "key:value";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;

    bool end = confNext(&opts, &k, &klen, &v, &vlen);
    CHECK(!end);
    CHECK_EQ(klen, size_t(3));
    CHECK(strncmp(k, "key", klen) == 0);
    CHECK_EQ(vlen, size_t(5));
    CHECK(strncmp(v, "value", vlen) == 0);
    CHECK(!had_conf_error);
    CHECK_EQ(*opts, '\0');
}

TEST(ConfNext, Multi)
{
    resetState();
    const char * opts = "k1:v1,k2:v2";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;

    CHECK(!confNext(&opts, &k, &klen, &v, &vlen));
    CHECK_EQ(klen, size_t(2));
    CHECK(strncmp(k, "k1", klen) == 0);
    CHECK_EQ(vlen, size_t(2));
    CHECK(strncmp(v, "v1", vlen) == 0);

    CHECK(!confNext(&opts, &k, &klen, &v, &vlen));
    CHECK_EQ(klen, size_t(2));
    CHECK(strncmp(k, "k2", klen) == 0);
    CHECK_EQ(vlen, size_t(2));
    CHECK(strncmp(v, "v2", vlen) == 0);

    CHECK(!had_conf_error);
}

TEST(ConfNext, Empty)
{
    resetState();
    const char * opts = "";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(confNext(&opts, &k, &klen, &v, &vlen));
    CHECK(!had_conf_error);
    CHECK_STREQ(messages, "");
}

TEST(ConfNext, MissingValue)
{
    resetState();
    const char * opts = "key_only";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(confNext(&opts, &k, &klen, &v, &vlen));
    CHECK(had_conf_error);
    CHECK_STREQ(messages, "<jemalloc>: Conf string ends with key -- key_only\n");
}

TEST(ConfNext, Malformed)
{
    resetState();
    const char * opts = "bad!key:val";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(confNext(&opts, &k, &klen, &v, &vlen));
    CHECK(had_conf_error);
    CHECK_STREQ(messages, "<jemalloc>: Malformed conf string -- bad!\n");
}

TEST(ConfNext, TrailingComma)
{
    resetState();
    const char * opts = "k:v,";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(!confNext(&opts, &k, &klen, &v, &vlen));
    CHECK(had_conf_error);
    CHECK_EQ(vlen, size_t(1));
    CHECK_STREQ(messages, "<jemalloc>: Conf string ends with comma -- k:v,\n");
}

TEST(ConfNext, EmptyKeyAndValue)
{
    resetState();
    const char * opts = ":,a:b:c";
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(!confNext(&opts, &k, &klen, &v, &vlen));
    CHECK_EQ(klen, size_t(0));
    CHECK_EQ(vlen, size_t(0));
    CHECK(!confNext(&opts, &k, &klen, &v, &vlen));
    CHECK_EQ(klen, size_t(1));
    /// Values may contain ':'.
    CHECK_EQ(vlen, size_t(3));
    CHECK(strncmp(v, "b:c", vlen) == 0);
    CHECK(!had_conf_error);
}

TEST(ConfNext, LongErrorIsTruncated)
{
    resetState();
    char long_key[200];
    memset(long_key, 'k', sizeof(long_key) - 1);
    long_key[sizeof(long_key) - 1] = '\0';
    const char * opts = long_key;
    const char * k;
    size_t klen;
    const char * v;
    size_t vlen;
    CHECK(confNext(&opts, &k, &klen, &v, &vlen));
    /// BUFERROR_BUF (64) characters of the key.
    char expected[200];
    format(expected, sizeof(expected), "<jemalloc>: Conf string ends with key -- %.*s\n", 64, long_key);
    CHECK_STREQ(messages, expected);
}

/// --- test/unit/conf_parse.c ----------------------------------------------------------------------------------------

TEST(ConfParse, Bool)
{
    bool result = false;
    CHECK(!confHandleBool("true", 4, &result));
    CHECK(result);
    result = true;
    CHECK(!confHandleBool("false", 5, &result));
    CHECK(!result);
    CHECK(confHandleBool("yes", 3, &result));
    /// Only the first `vlen` characters are considered.
    CHECK(!confHandleBool("truex", 4, &result));
    CHECK(result);
    CHECK(confHandleBool("tru", 3, &result));
}

TEST(ConfParse, Unsigned)
{
    uintmax_t result = 0;
    CHECK(!confHandleUnsigned("100", 3, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(100));
    CHECK(!confHandleUnsigned("9999", 4, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(2048));
    CHECK(!confHandleUnsigned("0", 1, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(1));
    CHECK(confHandleUnsigned("9999", 4, 1, 2048, true, true, false, &result));
    CHECK(confHandleUnsigned("abc", 3, 1, 2048, true, true, true, &result));
    /// Base detection and the trailing-character check.
    CHECK(!confHandleUnsigned("0x10", 4, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), uint64_t(16));
    CHECK(!confHandleUnsigned("010", 3, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), uint64_t(8));
    /// "08": the conversion stops after the "0", so the value is not fully consumed.
    CHECK(confHandleUnsigned("08", 2, 0, 0, false, false, false, &result));
    CHECK(confHandleUnsigned("18446744073709551616", 20, 0, 0, false, false, false, &result));
    CHECK(!confHandleUnsigned("-1", 2, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), UINT64_MAX);
}

TEST(ConfParse, Signed)
{
    intmax_t result = 0;
    CHECK(!confHandleSigned("5000", 4, -1, INTMAX_MAX, true, false, false, &result));
    CHECK_EQ(int64_t(result), int64_t(5000));
    CHECK(!confHandleSigned("-1", 2, -1, INTMAX_MAX, true, false, false, &result));
    CHECK_EQ(int64_t(result), int64_t(-1));
    CHECK(confHandleSigned("5000", 4, -1, 4999, true, true, false, &result));
    CHECK(confHandleSigned("-2", 2, -1, 4999, true, true, false, &result));
    CHECK(!confHandleSigned("-2", 2, -1, 4999, true, true, true, &result));
    CHECK_EQ(int64_t(result), int64_t(-1));
}

TEST(ConfParse, CharP)
{
    char buf[8];
    CHECK(!confHandleCharP("hello", 5, buf, sizeof(buf)));
    CHECK_STREQ(buf, "hello");
    CHECK(!confHandleCharP("longstring", 10, buf, sizeof(buf)));
    CHECK_STREQ(buf, "longstr");
}

TEST(ConfParse, MultiSetting)
{
    const char * v = "1-2:3|0x10-020:4";
    const char * cur = v;
    size_t len_left = strlen(v);
    size_t start;
    size_t end;
    size_t value;
    CHECK(!multiSettingParseNext(&cur, &len_left, &start, &end, &value));
    CHECK_EQ(start, size_t(1));
    CHECK_EQ(end, size_t(2));
    CHECK_EQ(value, size_t(3));
    CHECK_EQ(len_left, size_t(10));
    CHECK(!multiSettingParseNext(&cur, &len_left, &start, &end, &value));
    CHECK_EQ(start, size_t(16));
    CHECK_EQ(end, size_t(16));
    CHECK_EQ(value, size_t(4));
    CHECK_EQ(len_left, size_t(0));

    cur = "1-2";
    len_left = 3;
    CHECK(multiSettingParseNext(&cur, &len_left, &start, &end, &value));
    cur = "1:2-3";
    len_left = 5;
    CHECK(multiSettingParseNext(&cur, &len_left, &start, &end, &value));
}

/// --- test/unit/malloc_conf_2.c (with MALLOC_CONF="dirty_decay_ms:500" from malloc_conf_2.sh) -----------------------

TEST(Conf, MallocConf2)
{
    resetState();
    je_malloc_conf = "dirty_decay_ms:1000,muzzy_decay_ms:2000";
    je_malloc_conf_2_conf_harder = "dirty_decay_ms:1234";
    setenv("MALLOC_CONF", "dirty_decay_ms:500", 1);
    Boot boot;
    boot.run();
    CHECK_EQ(opt.dirty_decay_ms, ssize_t(1234));
    CHECK_EQ(opt.muzzy_decay_ms, ssize_t(2000));
    CHECK_STREQ(opt.malloc_conf_env_var, "dirty_decay_ms:500");
    CHECK(opt.malloc_conf_symlink == nullptr);
    CHECK_STREQ(boot.readlink_buf, "");
    CHECK_STREQ(messages, "");
    resetState();
}

/// --- Defaults ------------------------------------------------------------------------------------------------------

TEST(Options, CompiledDefaults)
{
    Options o{};
    CHECK(!o.abort);
    CHECK(!o.abort_conf);
    CHECK(!o.confirm_conf);
    CHECK_STREQ(o.junk, "false");
    CHECK_EQ(o.trust_madvise, !config::os_linux);
    CHECK(o.cache_oblivious);
    CHECK(o.zero_realloc_action == (config::os_linux ? ZeroReallocAction::Free : ZeroReallocAction::Alloc));
    CHECK(o.disable_large_size_classes);
    CHECK(o.experimental_tcache_gc);
    CHECK_EQ(o.narenas, 0u);
    CHECK_EQ(o.narenas_ratio, FixedPoint(4 << 16));
    CHECK_EQ(o.debug_double_free_max_scan, 32u);
    CHECK_EQ(o.calloc_madvise_threshold, size_t(8) << 20);
    CHECK(!o.hpa);
    CHECK_EQ(o.hpa_opts.slab_max_alloc, size_t(65536));
    CHECK_EQ(o.hpa_opts.hugification_threshold, HUGEPAGE * 95 / 100);
    CHECK_EQ(o.hpa_opts.dirty_mult, fxp::initPercent(25));
    CHECK_EQ(o.hpa_opts.hugify_delay_ms, uint64_t(10000));
    CHECK_EQ(o.hpa_opts.min_purge_interval_ms, uint64_t(5000));
    CHECK_EQ(o.hpa_opts.experimental_max_purge_nhp, ssize_t(-1));
    CHECK_EQ(o.hpa_opts.purge_threshold, PAGE);
    CHECK(o.hpa_opts.hugify_style == HpaHugifyStyle::Lazy);
    CHECK_EQ(o.hpa_sec_opts.nshards, size_t(2));
    CHECK_EQ(o.hpa_sec_opts.max_alloc, PAGE > 32768 ? PAGE : size_t(32768));
    CHECK_EQ(o.hpa_sec_opts.max_bytes, 4 * o.hpa_sec_opts.max_alloc > 262144 ? 4 * o.hpa_sec_opts.max_alloc : size_t(262144));
    CHECK_EQ(o.hpa_sec_opts.batch_fill_extra, size_t(3));
    CHECK(o.experimental_hpa_start_huge_if_thp_always);
    CHECK(!o.experimental_hpa_enforce_hugify);
    CHECK(o.percpu_arena == PercpuArenaMode::Disabled);
    CHECK_EQ(o.dirty_decay_ms, ssize_t(10000));
    CHECK_EQ(o.muzzy_decay_ms, ssize_t(0));
    CHECK_EQ(o.oversize_threshold, size_t(8) << 20);
    CHECK(o.metadata_thp == MetadataThpMode::Disabled);
    CHECK(o.thp == ThpMode::DoNothing);
    CHECK_EQ(o.retain, config::os_linux);
    CHECK_STREQ(o.dss, "secondary");
    CHECK_EQ(o.lg_extent_max_active_fit, size_t(6));
    CHECK_EQ(o.mutex_max_spin, int64_t(600));
    CHECK_STREQ(o.stats_print_opts, "");
    CHECK_EQ(o.stats_interval, int64_t(-1));
    CHECK(o.tcache);
    CHECK_EQ(o.tcache_max, size_t(32768));
    CHECK_EQ(o.tcache_nslots_small_min, 20u);
    CHECK_EQ(o.tcache_nslots_small_max, 200u);
    CHECK_EQ(o.tcache_nslots_large, 20u);
    CHECK_EQ(o.lg_tcache_nslots_mul, ssize_t(1));
    CHECK_EQ(o.tcache_gc_incr_bytes, size_t(65536));
    CHECK_EQ(o.lg_tcache_flush_small_div, 1u);
    CHECK(!o.background_thread);
    CHECK_EQ(o.max_background_threads, size_t(4096));
    CHECK(!o.prof);
    CHECK(o.prof_active);
    CHECK(o.prof_thread_active_init);
    CHECK_EQ(o.prof_bt_max, 128u);
    CHECK_EQ(o.lg_prof_sample, size_t(19));
    CHECK_EQ(o.lg_prof_interval, ssize_t(-1));
    CHECK_STREQ(o.prof_prefix, "jeprof");
    CHECK(o.prof_unbias);
    CHECK_EQ(o.prof_recent_alloc_max, ssize_t(0));
    CHECK(o.prof_time_res == ProfTimeRes::Default);
    CHECK_EQ(o.lg_san_uaf_align, ssize_t(-1));
    CHECK(o.malloc_conf_env_var == nullptr);

    CHECK_STREQ(percpu_arena_mode_names[unsigned(PercpuArenaMode::Disabled)], "disabled");
    CHECK_STREQ(percpu_arena_mode_names[unsigned(PercpuArenaMode::Percpu)], "percpu");
    CHECK_STREQ(percpu_arena_mode_names[unsigned(PercpuArenaMode::PerPhycpu)], "phycpu");
    CHECK_STREQ(zero_realloc_mode_names[unsigned(ZeroReallocAction::Abort)], "abort");
    CHECK_STREQ(hpa_hugify_style_names[unsigned(HpaHugifyStyle::Lazy)], "lazy");
    CHECK_STREQ(prof_time_res_mode_names[1], "high");
    CHECK_EQ(TCACHE_MAXCLASS_LIMIT, size_t(1) << (LG_PAGE + 3));
    CHECK_EQ(TCACHE_NBINS_MAX, SC_NBINS + 5);
}

TEST(Conf, ClickHouseConfiguration)
{
    /// The compiled-in ClickHouse configuration string.
    resetState();
    Boot boot;
    boot.run();
    CHECK_STREQ(messages, "");
    CHECK(!had_conf_error);
    if (strcmp(config::malloc_conf_default, "percpu_arena:percpu,oversize_threshold:67108864,muzzy_decay_ms:0,dirty_decay_ms:5000,"
            "prof:true,prof_active:true,prof_thread_active_init:false,background_thread:true,lg_extent_max_active_fit:6") == 0)
    {
        CHECK(opt.percpu_arena == PercpuArenaMode::PercpuUninit);
        CHECK_EQ(opt.oversize_threshold, size_t(64) << 20);
        CHECK_EQ(opt.muzzy_decay_ms, ssize_t(0));
        CHECK_EQ(opt.dirty_decay_ms, ssize_t(5000));
        CHECK(opt.prof);
        CHECK(opt.prof_active);
        CHECK(!opt.prof_thread_active_init);
        CHECK(opt.background_thread);
        CHECK_EQ(opt.lg_extent_max_active_fit, size_t(6));
    }
    /// Forced to 0 because jemalloc's `config_debug` is false.
    CHECK_EQ(opt.debug_double_free_max_scan, 0u);
    CHECK(opt.malloc_conf_env_var == nullptr);
    resetState();
}

/// --- The option table ----------------------------------------------------------------------------------------------

TEST(Conf, Errors)
{
    parseEnv("narenas:x,abort:yes,narenas:0,unknown:1,experimental_x:1");
    CHECK_STREQ(messages,
        "<jemalloc>: Invalid conf value: narenas:x\n"
        "<jemalloc>: Invalid conf value: abort:yes\n"
        "<jemalloc>: Out-of-range conf value: narenas:0\n"
        "<jemalloc>: Invalid conf pair: unknown:1\n"
        "<jemalloc>: Invalid conf pair: experimental_x:1\n");
    CHECK(had_conf_error);
    CHECK_EQ(opt.narenas, 0u);

    /// Experimental and deprecated keys do not set `had_conf_error`.
    parseEnv("experimental_x:1,hpa_sec_bytes_after_flush:1");
    CHECK(!had_conf_error);
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf pair: experimental_x:1\n<jemalloc>: Invalid conf pair: hpa_sec_bytes_after_flush:1\n");

    /// Syntax errors are reported in both passes; the rest of the source is ignored.
    parseEnv("narenas:3,a!:1,narenas:4");
    CHECK_STREQ(messages, "<jemalloc>: Malformed conf string -- a!\n<jemalloc>: Malformed conf string -- a!\n");
    CHECK_EQ(opt.narenas, 3u);
    resetState();
}

TEST(Conf, Numbers)
{
    parseEnv("narenas:4294967297");
    /// Truncating cast to `unsigned`.
    CHECK_EQ(opt.narenas, 1u);
    parseEnv("narenas:3,narenas:default");
    CHECK_EQ(opt.narenas, 0u);
    parseEnv("dirty_decay_ms:-1,muzzy_decay_ms:18446744072000");
    CHECK_EQ(opt.dirty_decay_ms, ssize_t(-1));
    CHECK_EQ(opt.muzzy_decay_ms, ssize_t(18446744072000));
    parseEnv("dirty_decay_ms:-2");
    /// The value from the compiled-in conf, which is empty on s390x and FreeBSD ppc64le (then the default 10000).
    CHECK_EQ(opt.dirty_decay_ms, ssize_t(std::strstr(config::malloc_conf_default, "dirty_decay_ms:5000") ? 5000 : 10000));
    CHECK_STREQ(messages, "<jemalloc>: Out-of-range conf value: dirty_decay_ms:-2\n");
    parseEnv("tcache_max:1000000000");
    CHECK_EQ(opt.tcache_max, TCACHE_MAXCLASS_LIMIT);
    parseEnv("lg_tcache_max:100");
    CHECK_EQ(opt.tcache_max, TCACHE_MAXCLASS_LIMIT);
    parseEnv("lg_tcache_max:10");
    CHECK_EQ(opt.tcache_max, size_t(1024));
    parseEnv("tcache_nslots_small_min:0,tcache_nslots_large:5000");
    CHECK_EQ(opt.tcache_nslots_small_min, 1u);
    CHECK_EQ(opt.tcache_nslots_large, 2048u);
    parseEnv("max_background_threads:5000");
    CHECK_EQ(opt.max_background_threads, size_t(4096));
    parseEnv("max_background_threads:10,max_background_threads:20");
    /// The maximum is the current value.
    CHECK_EQ(opt.max_background_threads, size_t(10));
    parseEnv("prof_bt_max:4294967296");
    CHECK_EQ(opt.prof_bt_max, UINT_MAX);
    parseEnv("mutex_max_spin:9223372036854775808");
    CHECK_STREQ(messages, "<jemalloc>: Out-of-range conf value: mutex_max_spin:9223372036854775808\n");
    parseEnv("debug_double_free_max_scan:100");
    CHECK_EQ(opt.debug_double_free_max_scan, 0u);
    parseEnv("narenas_ratio:2.5");
    CHECK_EQ(opt.narenas_ratio, FixedPoint((2 << 16) + (1 << 15)));
    parseEnv("hpa_hugification_threshold_ratio:0.5,hpa_dirty_mult:-1");
    CHECK_EQ(opt.hpa_opts.hugification_threshold, HUGEPAGE / 2);
    CHECK_EQ(opt.hpa_opts.dirty_mult, FixedPoint(-1));
    resetState();
}

TEST(Conf, PrefixQuirks)
{
    parseEnv("m:a");
    CHECK(opt.metadata_thp == MetadataThpMode::Auto);
    parseEnv(":al");
    CHECK(opt.metadata_thp == MetadataThpMode::Always);
    parseEnv("d:p");
    CHECK_STREQ(opt.dss, "primary");
    CHECK(extentDssPrecGet() == (config::have_dss ? DssPrec::Primary : DssPrec::Disabled));
    parseEnv("dss:");
    CHECK_STREQ(opt.dss, "disabled");
    parseEnv("p:ph");
    CHECK(opt.percpu_arena == PercpuArenaMode::PerPhycpuUninit);
    parseEnv("percpu_arena:p");
    CHECK(opt.percpu_arena == PercpuArenaMode::PercpuUninit);
    parseEnv("hpa_:e");
    CHECK(opt.hpa_opts.hugify_style == HpaHugifyStyle::Eager);
    parseEnv("thp:n");
    CHECK(opt.thp == ThpMode::Never);
    /// `thp` is an exact key.
    parseEnv("th:n");
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf pair: th:n\n");
    resetState();
}

TEST(Conf, Junk)
{
    parseEnv("junk:alloc");
    CHECK_STREQ(opt.junk, "alloc");
    CHECK(opt.junk_alloc);
    CHECK(!opt.junk_free);
    parseEnv("junk:free");
    CHECK_STREQ(opt.junk, "free");
    CHECK(!opt.junk_alloc);
    CHECK(opt.junk_free);
    parseEnv("junk:true");
    CHECK_STREQ(opt.junk, "true");
    CHECK(opt.junk_alloc && opt.junk_free);
    parseEnv("junk:true,junk:fals");
    CHECK_STREQ(opt.junk, "true");
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf value: junk:fals\n");
    resetState();
}

TEST(Conf, StatsOpts)
{
    parseEnv("stats_print_opts:JJxzg,stats_print_opts:gm,stats_interval_opts:hh");
    CHECK_STREQ(opt.stats_print_opts, "Jxgm");
    CHECK_STREQ(opt.stats_interval_opts, "h");
    resetState();
}

TEST(Conf, ProfPrefix)
{
    static char conf[PATH_MAX + 100];
    memcpy(conf, "prof_prefix:", 12);
    memset(conf + 12, 'p', PATH_MAX + 10);
    conf[12 + PATH_MAX + 10] = '\0';
    parseEnv(conf);
    CHECK_EQ(strlen(opt.prof_prefix), size_t(PATH_MAX));
    parseEnv("prof_prefix:/tmp/x");
    CHECK_STREQ(opt.prof_prefix, "/tmp/x");
    resetState();
}

TEST(Conf, SlabSizesAndBinShards)
{
    Boot boot;
    parseEnv("slab_sizes:1-4096:1,bin_shards:1-160:16|8-8:2", boot);
    CHECK_STREQ(messages, "");
    for (int i = 0; i < boot.sc_data.nbins; ++i)
    {
        size_t reg_size = regSizeCompute(boot.sc_data.sc[i].lg_base, boot.sc_data.sc[i].lg_delta, boot.sc_data.sc[i].ndelta);
        if (reg_size <= 4096)
            CHECK_EQ(boot.sc_data.sc[i].pgs, 1);
        else
            CHECK_EQ(boot.sc_data.sc[i].pgs, default_sc_data.sc[i].pgs);
    }
    CHECK_EQ(boot.bin_shard_sizes[0], 2u);
    CHECK_EQ(boot.bin_shard_sizes[sz::sizeToIndexCompute(160)], 16u);
    CHECK_EQ(boot.bin_shard_sizes[sz::sizeToIndexCompute(160) + 1], 1u);

    parseEnv("slab_sizes:1-4096:1,slab_sizes:default", boot);
    for (int i = 0; i < boot.sc_data.nbins; ++i)
        CHECK_EQ(boot.sc_data.sc[i].pgs, default_sc_data.sc[i].pgs);

    parseEnv("bin_shards:1-160:65", boot);
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for bin_shards: bin_shards:1-160:65\n");
    CHECK_EQ(boot.bin_shard_sizes[0], 1u);

    /// The first segment is applied before the error.
    parseEnv("slab_sizes:4096-4096:3|x", boot);
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for slab_sizes: slab_sizes:4096-4096:3|x\n");
    CHECK_EQ(boot.sc_data.sc[sz::sizeToIndexCompute(4096)].pgs, 3);
    resetState();
}

TEST(Conf, TcacheNcachedMax)
{
    parseEnv("tcache_ncached_max:1-16:100|100-50:7|8-8:100000");
    CHECK_STREQ(messages, "");
    CHECK_EQ(opt.tcache_ncached_max[0], uint16_t(8191));
    CHECK(opt.tcache_ncached_max_set[0]);
    CHECK_EQ(opt.tcache_ncached_max[1], uint16_t(100));
    CHECK(opt.tcache_ncached_max_set[1]);
    CHECK(!opt.tcache_ncached_max_set[2]);

    parseEnv("tcache_ncached_max:0-100000000:5");
    for (unsigned i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        CHECK(opt.tcache_ncached_max_set[i]);
        CHECK_EQ(opt.tcache_ncached_max[i], uint16_t(5));
    }

    parseEnv("tcache_ncached_max:1-2");
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for tcache_ncached_max: tcache_ncached_max:1-2\n");
    resetState();
}

TEST(Conf, ConfirmConf)
{
    parseEnv("narenas:3,abort:x,confirm_conf:true");
    char expected[4096];
    format(expected, sizeof(expected),
        "<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): \"%s\"\n%s"
        "<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): \"\"\n"
        "<jemalloc>: malloc_conf #3 (\"name\" of the file referenced by the symbolic link named /etc/malloc.conf): \"\"\n"
        "<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): \"narenas:3,abort:x,confirm_conf:true\"\n"
        "<jemalloc>: -- Set conf value: narenas:3\n"
        "<jemalloc>: Invalid conf value: abort:x\n"
        "<jemalloc>: -- Set conf value: confirm_conf:true\n"
        "<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): \"\"\n",
        config::malloc_conf_default, "");
    /// Remove the "-- Set conf value" lines of the compiled-in string from `messages` before comparing.
    const char * env_header = strstr(messages, "<jemalloc>: malloc_conf #2");
    const char * expected_env_header = strstr(expected, "<jemalloc>: malloc_conf #2");
    REQUIRE(env_header && expected_env_header);
    CHECK_STREQ(env_header, expected_env_header);
    CHECK(strncmp(messages, "<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): \"", 71) == 0);
    resetState();
}

TEST(Conf, AbortConf)
{
    /// An error with `abort_conf:true` aborts after the source has been processed.
    int sig = runInChild(
        []
        {
            je_malloc_message = nullptr;
            int devnull = open("/dev/null", O_WRONLY);
            dup2(devnull, 2);
            parseEnv("abort_conf:true,narenas:x");
        });
    CHECK_EQ(sig, SIGABRT);

    /// Experimental keys are tolerated.
    sig = runInChild([] { parseEnv("abort_conf:true,experimental_x:1"); });
    CHECK_EQ(sig, 0);

    /// `prof_leak_error` without `prof_final`.
    sig = runInChild([] { parseEnv("abort_conf:true,prof_leak_error:true"); });
    CHECK_EQ(sig, SIGABRT);
    parseEnv("prof_leak_error:true");
    CHECK_STREQ(messages, "<jemalloc>: prof_leak_error is set w/o prof_final.\n");
    CHECK(!had_conf_error);
    resetState();
}

TEST(Conf, Hpa)
{
    parseEnv("hpa:true,hpa_slab_max_alloc:131072");
    CHECK(opt.hpa);
    CHECK_EQ(opt.hpa_opts.slab_max_alloc, size_t(131072));
    hpaDisableUnsupported();
    CHECK(!opt.hpa);
    CHECK_STREQ(messages, "<jemalloc>: HPA not supported in the current configuration; disabling.");

    int sig = runInChild(
        []
        {
            parseEnv("hpa:true,abort_conf:true");
            hpaDisableUnsupported();
        });
    CHECK_EQ(sig, SIGABRT);
    resetState();
}
