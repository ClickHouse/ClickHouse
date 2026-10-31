/// Tests of the configuration parser and the option defaults.
/// Ports of jemalloc's `test/unit/conf.c`, `test/unit/conf_parse.c`, `test/unit/malloc_conf_2.c`, plus pinned
/// behavior of the option table (values from jemalloc; the exhaustive comparison is in malloc_conf_oracle.cpp).

#include <allocator/ExtentHooks.h>
#include <allocator/Format.h>
#include <allocator/MallocConf.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <climits>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

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
    options = Options{};
    had_configuration_error = false;
    extentSbrkPrecedenceSet(SBRK_PRECEDENCE_DEFAULT);
    messages_len = 0;
    messages[0] = '\0';
    je_malloc_message = captureMessage;
    je_malloc_conf = nullptr;
    je_malloc_conf_2_conf_harder = nullptr;
    unsetenv("MALLOC_CONF");
}

struct Boot
{
    SizeClassData size_class_data{};
    unsigned bin_shard_sizes[SIZE_CLASS_NUM_BINS];
    char readlink_buf[MALLOC_CONF_READLINK_BUF_SIZE];

    /// The boot steps that precede `malloc_conf_init` and the parser itself.
    void run()
    {
        sizeClassBoot(size_class_data);
        binShardSizesBoot(bin_shard_sizes);
        readlink_buf[0] = '\0';
        mallocConfInit(size_class_data, bin_shard_sizes, readlink_buf);
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

TEST(ConfigurationNext, Simple)
{
    resetState();
    const char * options_string = "key:value";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;

    bool end = configurationNext(&options_string, &k, &key_length, &v, &value_length);
    CHECK(!end);
    CHECK_EQ(key_length, size_t(3));
    CHECK(strncmp(k, "key", key_length) == 0);
    CHECK_EQ(value_length, size_t(5));
    CHECK(strncmp(v, "value", value_length) == 0);
    CHECK(!had_configuration_error);
    CHECK_EQ(*options_string, '\0');
}

TEST(ConfigurationNext, Multi)
{
    resetState();
    const char * options_string = "k1:v1,k2:v2";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;

    CHECK(!configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK_EQ(key_length, size_t(2));
    CHECK(strncmp(k, "k1", key_length) == 0);
    CHECK_EQ(value_length, size_t(2));
    CHECK(strncmp(v, "v1", value_length) == 0);

    CHECK(!configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK_EQ(key_length, size_t(2));
    CHECK(strncmp(k, "k2", key_length) == 0);
    CHECK_EQ(value_length, size_t(2));
    CHECK(strncmp(v, "v2", value_length) == 0);

    CHECK(!had_configuration_error);
}

TEST(ConfigurationNext, Empty)
{
    resetState();
    const char * options_string = "";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK(!had_configuration_error);
    CHECK_STREQ(messages, "");
}

TEST(ConfigurationNext, MissingValue)
{
    resetState();
    const char * options_string = "key_only";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK(had_configuration_error);
    CHECK_STREQ(messages, "<jemalloc>: Conf string ends with key -- key_only\n");
}

TEST(ConfigurationNext, Malformed)
{
    resetState();
    const char * options_string = "bad!key:val";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK(had_configuration_error);
    CHECK_STREQ(messages, "<jemalloc>: Malformed conf string -- bad!\n");
}

TEST(ConfigurationNext, TrailingComma)
{
    resetState();
    const char * options_string = "k:v,";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(!configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK(had_configuration_error);
    CHECK_EQ(value_length, size_t(1));
    CHECK_STREQ(messages, "<jemalloc>: Conf string ends with comma -- k:v,\n");
}

TEST(ConfigurationNext, EmptyKeyAndValue)
{
    resetState();
    const char * options_string = ":,a:b:c";
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(!configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK_EQ(key_length, size_t(0));
    CHECK_EQ(value_length, size_t(0));
    CHECK(!configurationNext(&options_string, &k, &key_length, &v, &value_length));
    CHECK_EQ(key_length, size_t(1));
    /// Values may contain ':'.
    CHECK_EQ(value_length, size_t(3));
    CHECK(strncmp(v, "b:c", value_length) == 0);
    CHECK(!had_configuration_error);
}

TEST(ConfigurationNext, LongErrorIsTruncated)
{
    resetState();
    char long_key[200];
    memset(long_key, 'k', sizeof(long_key) - 1);
    long_key[sizeof(long_key) - 1] = '\0';
    const char * options_string = long_key;
    const char * k;
    size_t key_length;
    const char * v;
    size_t value_length;
    CHECK(configurationNext(&options_string, &k, &key_length, &v, &value_length));
    /// BUF_ERROR_BUF (64) characters of the key.
    char expected[200];
    format(expected, sizeof(expected), "<jemalloc>: Conf string ends with key -- %.*s\n", 64, long_key);
    CHECK_STREQ(messages, expected);
}

/// --- test/unit/conf_parse.c ----------------------------------------------------------------------------------------

TEST(ConfigurationParse, Bool)
{
    bool result = false;
    CHECK(!configurationHandleBool("true", 4, &result));
    CHECK(result);
    result = true;
    CHECK(!configurationHandleBool("false", 5, &result));
    CHECK(!result);
    CHECK(configurationHandleBool("yes", 3, &result));
    /// Only the first `value_length` characters are considered.
    CHECK(!configurationHandleBool("truex", 4, &result));
    CHECK(result);
    CHECK(configurationHandleBool("tru", 3, &result));
}

TEST(ConfigurationParse, Unsigned)
{
    uintmax_t result = 0;
    CHECK(!configurationHandleUnsigned("100", 3, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(100));
    CHECK(!configurationHandleUnsigned("9999", 4, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(2048));
    CHECK(!configurationHandleUnsigned("0", 1, 1, 2048, true, true, true, &result));
    CHECK_EQ(uint64_t(result), uint64_t(1));
    CHECK(configurationHandleUnsigned("9999", 4, 1, 2048, true, true, false, &result));
    CHECK(configurationHandleUnsigned("abc", 3, 1, 2048, true, true, true, &result));
    /// Base detection and the trailing-character check.
    CHECK(!configurationHandleUnsigned("0x10", 4, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), uint64_t(16));
    CHECK(!configurationHandleUnsigned("010", 3, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), uint64_t(8));
    /// "08": the conversion stops after the "0", so the value is not fully consumed.
    CHECK(configurationHandleUnsigned("08", 2, 0, 0, false, false, false, &result));
    CHECK(configurationHandleUnsigned("18446744073709551616", 20, 0, 0, false, false, false, &result));
    CHECK(!configurationHandleUnsigned("-1", 2, 0, 0, false, false, false, &result));
    CHECK_EQ(uint64_t(result), UINT64_MAX);
}

TEST(ConfigurationParse, Signed)
{
    intmax_t result = 0;
    CHECK(!configurationHandleSigned("5000", 4, -1, INTMAX_MAX, true, false, false, &result));
    CHECK_EQ(int64_t(result), int64_t(5000));
    CHECK(!configurationHandleSigned("-1", 2, -1, INTMAX_MAX, true, false, false, &result));
    CHECK_EQ(int64_t(result), int64_t(-1));
    CHECK(configurationHandleSigned("5000", 4, -1, 4999, true, true, false, &result));
    CHECK(configurationHandleSigned("-2", 2, -1, 4999, true, true, false, &result));
    CHECK(!configurationHandleSigned("-2", 2, -1, 4999, true, true, true, &result));
    CHECK_EQ(int64_t(result), int64_t(-1));
}

TEST(ConfigurationParse, CharPtr)
{
    char buf[8];
    CHECK(!configurationHandleCharPtr("hello", 5, buf, sizeof(buf)));
    CHECK_STREQ(buf, "hello");
    CHECK(!configurationHandleCharPtr("longstring", 10, buf, sizeof(buf)));
    CHECK_STREQ(buf, "longstr");
}

TEST(ConfigurationParse, MultiSetting)
{
    const char * v = "1-2:3|0x10-020:4";
    const char * current = v;
    size_t len_left = strlen(v);
    size_t start;
    size_t end;
    size_t value;
    CHECK(!multiSettingParseNext(&current, &len_left, &start, &end, &value));
    CHECK_EQ(start, size_t(1));
    CHECK_EQ(end, size_t(2));
    CHECK_EQ(value, size_t(3));
    CHECK_EQ(len_left, size_t(10));
    CHECK(!multiSettingParseNext(&current, &len_left, &start, &end, &value));
    CHECK_EQ(start, size_t(16));
    CHECK_EQ(end, size_t(16));
    CHECK_EQ(value, size_t(4));
    CHECK_EQ(len_left, size_t(0));

    current = "1-2";
    len_left = 3;
    CHECK(multiSettingParseNext(&current, &len_left, &start, &end, &value));
    current = "1:2-3";
    len_left = 5;
    CHECK(multiSettingParseNext(&current, &len_left, &start, &end, &value));
}

/// --- test/unit/malloc_conf_2.c (with MALLOC_CONF="dirty_decay_ms:500" from malloc_conf_2.sh) -----------------------

TEST(Configuration, MallocConf2)
{
    resetState();
    je_malloc_conf = "dirty_decay_ms:1000,muzzy_decay_ms:2000";
    je_malloc_conf_2_conf_harder = "dirty_decay_ms:1234";
    setenv("MALLOC_CONF", "dirty_decay_ms:500", 1);
    Boot boot;
    boot.run();
    CHECK_EQ(options.dirty_decay_ms, ssize_t(1234));
    CHECK_EQ(options.muzzy_decay_ms, ssize_t(2000));
    CHECK_STREQ(options.malloc_conf_env_variable, "dirty_decay_ms:500");
    CHECK(options.malloc_conf_symlink == nullptr);
    CHECK_STREQ(boot.readlink_buf, "");
    CHECK_STREQ(messages, "");
    resetState();
}

/// --- Defaults ------------------------------------------------------------------------------------------------------

TEST(Options, CompiledDefaults)
{
    Options o{};
    CHECK(!o.abort);
    CHECK(!o.abort_configuration);
    CHECK(!o.confirm_configuration);
    CHECK_STREQ(o.junk, "false");
    CHECK_EQ(o.trust_madvise, !config::os_linux);
    CHECK(o.cache_oblivious);
    CHECK(o.zero_realloc_action == (config::os_linux ? ZeroReallocAction::Free : ZeroReallocAction::Alloc));
    CHECK(o.disable_large_size_classes);
    CHECK(o.experimental_thread_cache_gc);
    CHECK_EQ(o.num_arenas, 0u);
    CHECK_EQ(o.num_arenas_ratio, FixedPoint(4 << 16));
    CHECK_EQ(o.debug_double_free_max_scan, 32u);
    CHECK_EQ(o.calloc_madvise_threshold, size_t(8) << 20);
    CHECK(!o.huge_page_allocator);
    CHECK_EQ(o.huge_page_allocator_options.slab_max_alloc, size_t(65536));
    CHECK_EQ(o.huge_page_allocator_options.hugification_threshold, HUGE_PAGE * 95 / 100);
    CHECK_EQ(o.huge_page_allocator_options.dirty_multiplier, fixed_point::initPercent(25));
    CHECK_EQ(o.huge_page_allocator_options.hugify_delay_ms, uint64_t(10000));
    CHECK_EQ(o.huge_page_allocator_options.min_purge_interval_ms, uint64_t(5000));
    CHECK_EQ(o.huge_page_allocator_options.experimental_max_purge_num_huge_pages, ssize_t(-1));
    CHECK_EQ(o.huge_page_allocator_options.purge_threshold, PAGE);
    CHECK(o.huge_page_allocator_options.hugify_style == HugePageAllocatorHugifyStyle::Lazy);
    CHECK_EQ(o.small_extent_cache_options.num_shards, size_t(2));
    CHECK_EQ(o.small_extent_cache_options.max_alloc, PAGE > 32768 ? PAGE : size_t(32768));
    CHECK_EQ(
        o.small_extent_cache_options.max_bytes,
        4 * o.small_extent_cache_options.max_alloc > 262144 ? 4 * o.small_extent_cache_options.max_alloc : size_t(262144));
    CHECK_EQ(o.small_extent_cache_options.batch_fill_extra, size_t(3));
    CHECK(o.experimental_huge_page_allocator_start_huge);
    CHECK(!o.experimental_huge_page_allocator_enforce_hugify);
    CHECK(o.per_cpu_arena == PerCPUArenaMode::Disabled);
    CHECK_EQ(o.dirty_decay_ms, ssize_t(10000));
    CHECK_EQ(o.muzzy_decay_ms, ssize_t(0));
    CHECK_EQ(o.oversize_threshold, size_t(8) << 20);
    CHECK(o.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Disabled);
    CHECK(o.transparent_huge_pages == TransparentHugePagesMode::DoNothing);
    CHECK_EQ(o.retain, config::os_linux);
    CHECK_STREQ(o.sbrk, "secondary");
    CHECK_EQ(o.log2_extent_max_active_fit, size_t(6));
    CHECK_EQ(o.mutex_max_spin, int64_t(600));
    CHECK_STREQ(o.stats_print_options, "");
    CHECK_EQ(o.stats_interval, int64_t(-1));
    CHECK(o.thread_cache);
    CHECK_EQ(o.thread_cache_max, size_t(32768));
    CHECK_EQ(o.thread_cache_num_slots_small_min, 20u);
    CHECK_EQ(o.thread_cache_num_slots_small_max, 200u);
    CHECK_EQ(o.thread_cache_num_slots_large, 20u);
    CHECK_EQ(o.log2_thread_cache_num_slots_multiplier, ssize_t(1));
    CHECK_EQ(o.thread_cache_gc_increment_bytes, size_t(65536));
    CHECK_EQ(o.log2_thread_cache_flush_small_division, 1u);
    CHECK(!o.background_thread);
    CHECK_EQ(o.max_background_threads, size_t(4096));
    CHECK(!o.profiling);
    CHECK(o.profiling_active);
    CHECK(o.profiling_thread_active_init);
    CHECK_EQ(o.profiling_backtrace_max, 128u);
    CHECK_EQ(o.log2_profiling_sample, size_t(19));
    CHECK_EQ(o.log2_profiling_interval, ssize_t(-1));
    CHECK_STREQ(o.profiling_prefix, "jeprof");
    CHECK(o.profiling_unbias);
    CHECK_EQ(o.profiling_recent_alloc_max, ssize_t(0));
    CHECK(o.profiling_time_resolution == ProfilingTimeResolution::Default);
    CHECK_EQ(o.log2_sanitizer_use_after_free_align, ssize_t(-1));
    CHECK(o.malloc_conf_env_variable == nullptr);

    CHECK_STREQ(per_cpu_arena_mode_names[unsigned(PerCPUArenaMode::Disabled)], "disabled");
    CHECK_STREQ(per_cpu_arena_mode_names[unsigned(PerCPUArenaMode::PerCPU)], "percpu");
    CHECK_STREQ(per_cpu_arena_mode_names[unsigned(PerCPUArenaMode::PerPhysicalCPU)], "phycpu");
    CHECK_STREQ(zero_realloc_mode_names[unsigned(ZeroReallocAction::Abort)], "abort");
    CHECK_STREQ(huge_page_allocator_hugify_style_names[unsigned(HugePageAllocatorHugifyStyle::Lazy)], "lazy");
    CHECK_STREQ(profiling_time_resolution_mode_names[1], "high");
    CHECK_EQ(THREAD_CACHE_MAX_CLASS_LIMIT, size_t(1) << (LOG2_PAGE + 3));
    CHECK_EQ(THREAD_CACHE_NUM_BINS_MAX, SIZE_CLASS_NUM_BINS + 5);
}

TEST(Configuration, ClickHouseConfiguration)
{
    /// The compiled-in ClickHouse configuration string.
    resetState();
    Boot boot;
    boot.run();
    CHECK_STREQ(messages, "");
    CHECK(!had_configuration_error);
    if (strcmp(
            config::malloc_conf_default,
            "percpu_arena:percpu,oversize_threshold:67108864,muzzy_decay_ms:0,dirty_decay_ms:5000,"
            "prof:true,prof_active:true,prof_thread_active_init:false,background_thread:true,lg_extent_max_active_fit:6")
        == 0)
    {
        CHECK(options.per_cpu_arena == PerCPUArenaMode::PerCPUUninitialized);
        CHECK_EQ(options.oversize_threshold, size_t(64) << 20);
        CHECK_EQ(options.muzzy_decay_ms, ssize_t(0));
        CHECK_EQ(options.dirty_decay_ms, ssize_t(5000));
        CHECK(options.profiling);
        CHECK(options.profiling_active);
        CHECK(!options.profiling_thread_active_init);
        CHECK(options.background_thread);
        CHECK_EQ(options.log2_extent_max_active_fit, size_t(6));
    }
    /// Forced to 0 because jemalloc's `config_debug` is false.
    CHECK_EQ(options.debug_double_free_max_scan, 0u);
    CHECK(options.malloc_conf_env_variable == nullptr);
    resetState();
}

/// --- The option table ----------------------------------------------------------------------------------------------

TEST(Configuration, Errors)
{
    parseEnv("narenas:x,abort:yes,narenas:0,unknown:1,experimental_x:1");
    CHECK_STREQ(
        messages,
        "<jemalloc>: Invalid conf value: narenas:x\n"
        "<jemalloc>: Invalid conf value: abort:yes\n"
        "<jemalloc>: Out-of-range conf value: narenas:0\n"
        "<jemalloc>: Invalid conf pair: unknown:1\n"
        "<jemalloc>: Invalid conf pair: experimental_x:1\n");
    CHECK(had_configuration_error);
    CHECK_EQ(options.num_arenas, 0u);

    /// Experimental and deprecated keys do not set `had_configuration_error`.
    parseEnv("experimental_x:1,hpa_sec_bytes_after_flush:1");
    CHECK(!had_configuration_error);
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf pair: experimental_x:1\n<jemalloc>: Invalid conf pair: hpa_sec_bytes_after_flush:1\n");

    /// Syntax errors are reported in both passes; the rest of the source is ignored.
    parseEnv("narenas:3,a!:1,narenas:4");
    CHECK_STREQ(messages, "<jemalloc>: Malformed conf string -- a!\n<jemalloc>: Malformed conf string -- a!\n");
    CHECK_EQ(options.num_arenas, 3u);
    resetState();
}

TEST(Configuration, Numbers)
{
    parseEnv("narenas:4294967297");
    /// Truncating cast to `unsigned`.
    CHECK_EQ(options.num_arenas, 1u);
    parseEnv("narenas:3,narenas:default");
    CHECK_EQ(options.num_arenas, 0u);
    parseEnv("dirty_decay_ms:-1,muzzy_decay_ms:18446744072000");
    CHECK_EQ(options.dirty_decay_ms, ssize_t(-1));
    CHECK_EQ(options.muzzy_decay_ms, ssize_t(18446744072000));
    parseEnv("dirty_decay_ms:-2");
    /// The value from the compiled-in conf, which is empty on s390x and FreeBSD ppc64le (then the default 10000).
    CHECK_EQ(options.dirty_decay_ms, ssize_t(std::strstr(config::malloc_conf_default, "dirty_decay_ms:5000") ? 5000 : 10000));
    CHECK_STREQ(messages, "<jemalloc>: Out-of-range conf value: dirty_decay_ms:-2\n");
    parseEnv("tcache_max:1000000000");
    CHECK_EQ(options.thread_cache_max, THREAD_CACHE_MAX_CLASS_LIMIT);
    parseEnv("lg_tcache_max:100");
    CHECK_EQ(options.thread_cache_max, THREAD_CACHE_MAX_CLASS_LIMIT);
    parseEnv("lg_tcache_max:10");
    CHECK_EQ(options.thread_cache_max, size_t(1024));
    parseEnv("tcache_nslots_small_min:0,tcache_nslots_large:5000");
    CHECK_EQ(options.thread_cache_num_slots_small_min, 1u);
    CHECK_EQ(options.thread_cache_num_slots_large, 2048u);
    parseEnv("max_background_threads:5000");
    CHECK_EQ(options.max_background_threads, size_t(4096));
    parseEnv("max_background_threads:10,max_background_threads:20");
    /// The maximum is the current value.
    CHECK_EQ(options.max_background_threads, size_t(10));
    parseEnv("prof_bt_max:4294967296");
    CHECK_EQ(options.profiling_backtrace_max, UINT_MAX);
    parseEnv("mutex_max_spin:9223372036854775808");
    CHECK_STREQ(messages, "<jemalloc>: Out-of-range conf value: mutex_max_spin:9223372036854775808\n");
    parseEnv("debug_double_free_max_scan:100");
    CHECK_EQ(options.debug_double_free_max_scan, 0u);
    parseEnv("narenas_ratio:2.5");
    CHECK_EQ(options.num_arenas_ratio, FixedPoint((2 << 16) + (1 << 15)));
    parseEnv("hpa_hugification_threshold_ratio:0.5,hpa_dirty_mult:-1");
    CHECK_EQ(options.huge_page_allocator_options.hugification_threshold, HUGE_PAGE / 2);
    CHECK_EQ(options.huge_page_allocator_options.dirty_multiplier, FixedPoint(-1));
    resetState();
}

TEST(Configuration, PrefixQuirks)
{
    parseEnv("m:a");
    CHECK(options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Auto);
    parseEnv(":al");
    CHECK(options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Always);
    parseEnv("d:p");
    CHECK_STREQ(options.sbrk, "primary");
    CHECK(extentSbrkPrecedenceGet() == (config::have_sbrk ? SbrkPrecedence::Primary : SbrkPrecedence::Disabled));
    parseEnv("dss:");
    CHECK_STREQ(options.sbrk, "disabled");
    parseEnv("p:ph");
    CHECK(options.per_cpu_arena == PerCPUArenaMode::PerPhysicalCPUUninitialized);
    parseEnv("percpu_arena:p");
    CHECK(options.per_cpu_arena == PerCPUArenaMode::PerCPUUninitialized);
    parseEnv("hpa_:e");
    CHECK(options.huge_page_allocator_options.hugify_style == HugePageAllocatorHugifyStyle::Eager);
    parseEnv("thp:n");
    CHECK(options.transparent_huge_pages == TransparentHugePagesMode::Never);
    /// `transparent_huge_pages` is an exact key.
    parseEnv("th:n");
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf pair: th:n\n");
    resetState();
}

TEST(Configuration, Junk)
{
    parseEnv("junk:alloc");
    CHECK_STREQ(options.junk, "alloc");
    CHECK(options.junk_alloc);
    CHECK(!options.junk_free);
    parseEnv("junk:free");
    CHECK_STREQ(options.junk, "free");
    CHECK(!options.junk_alloc);
    CHECK(options.junk_free);
    parseEnv("junk:true");
    CHECK_STREQ(options.junk, "true");
    CHECK(options.junk_alloc && options.junk_free);
    parseEnv("junk:true,junk:fals");
    CHECK_STREQ(options.junk, "true");
    CHECK_STREQ(messages, "<jemalloc>: Invalid conf value: junk:fals\n");
    resetState();
}

TEST(Configuration, StatsOptions)
{
    parseEnv("stats_print_opts:JJxzg,stats_print_opts:gm,stats_interval_opts:hh");
    CHECK_STREQ(options.stats_print_options, "Jxgm");
    CHECK_STREQ(options.stats_interval_options, "h");
    resetState();
}

TEST(Configuration, ProfilingPrefix)
{
    static char configuration[PATH_MAX + 100];
    memcpy(configuration, "prof_prefix:", 12);
    memset(configuration + 12, 'p', PATH_MAX + 10);
    configuration[12 + PATH_MAX + 10] = '\0';
    parseEnv(configuration);
    CHECK_EQ(strlen(options.profiling_prefix), size_t(PATH_MAX));
    parseEnv("prof_prefix:/tmp/x");
    CHECK_STREQ(options.profiling_prefix, "/tmp/x");
    resetState();
}

TEST(Configuration, SlabSizesAndBinShards)
{
    Boot boot;
    parseEnv("slab_sizes:1-4096:1,bin_shards:1-160:16|8-8:2", boot);
    CHECK_STREQ(messages, "");
    for (int i = 0; i < boot.size_class_data.num_bins; ++i)
    {
        size_t region_size = regionSizeCompute(
            boot.size_class_data.size_class[i].log2_base,
            boot.size_class_data.size_class[i].log2_delta,
            boot.size_class_data.size_class[i].num_delta);
        if (region_size <= 4096)
            CHECK_EQ(boot.size_class_data.size_class[i].pages, 1);
        else
            CHECK_EQ(boot.size_class_data.size_class[i].pages, default_size_class_data.size_class[i].pages);
    }
    CHECK_EQ(boot.bin_shard_sizes[0], 2u);
    CHECK_EQ(boot.bin_shard_sizes[size_classes::sizeToIndexCompute(160)], 16u);
    CHECK_EQ(boot.bin_shard_sizes[size_classes::sizeToIndexCompute(160) + 1], 1u);

    parseEnv("slab_sizes:1-4096:1,slab_sizes:default", boot);
    for (int i = 0; i < boot.size_class_data.num_bins; ++i)
        CHECK_EQ(boot.size_class_data.size_class[i].pages, default_size_class_data.size_class[i].pages);

    parseEnv("bin_shards:1-160:65", boot);
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for bin_shards: bin_shards:1-160:65\n");
    CHECK_EQ(boot.bin_shard_sizes[0], 1u);

    /// The first segment is applied before the error.
    parseEnv("slab_sizes:4096-4096:3|x", boot);
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for slab_sizes: slab_sizes:4096-4096:3|x\n");
    CHECK_EQ(boot.size_class_data.size_class[size_classes::sizeToIndexCompute(4096)].pages, 3);
    resetState();
}

TEST(Configuration, ThreadCacheNumCachedMax)
{
    parseEnv("tcache_ncached_max:1-16:100|100-50:7|8-8:100000");
    CHECK_STREQ(messages, "");
    CHECK_EQ(options.thread_cache_num_cached_max[0], uint16_t(8191));
    CHECK(options.thread_cache_num_cached_max_set[0]);
    CHECK_EQ(options.thread_cache_num_cached_max[1], uint16_t(100));
    CHECK(options.thread_cache_num_cached_max_set[1]);
    CHECK(!options.thread_cache_num_cached_max_set[2]);

    parseEnv("tcache_ncached_max:0-100000000:5");
    for (unsigned i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        CHECK(options.thread_cache_num_cached_max_set[i]);
        CHECK_EQ(options.thread_cache_num_cached_max[i], uint16_t(5));
    }

    parseEnv("tcache_ncached_max:1-2");
    CHECK_STREQ(messages, "<jemalloc>: Invalid settings for tcache_ncached_max: tcache_ncached_max:1-2\n");
    resetState();
}

TEST(Configuration, ConfirmConfiguration)
{
    parseEnv("narenas:3,abort:x,confirm_conf:true");
    char expected[4096];
    format(
        expected,
        sizeof(expected),
        "<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): \"%s\"\n%s"
        "<jemalloc>: malloc_conf #2 (string pointed to by the global variable malloc_conf): \"\"\n"
        "<jemalloc>: malloc_conf #3 (\"name\" of the file referenced by the symbolic link named /etc/malloc.conf): \"\"\n"
        "<jemalloc>: malloc_conf #4 (value of the environment variable MALLOC_CONF): \"narenas:3,abort:x,confirm_conf:true\"\n"
        "<jemalloc>: -- Set conf value: narenas:3\n"
        "<jemalloc>: Invalid conf value: abort:x\n"
        "<jemalloc>: -- Set conf value: confirm_conf:true\n"
        "<jemalloc>: malloc_conf #5 (string pointed to by the global variable malloc_conf_2_conf_harder): \"\"\n",
        config::malloc_conf_default,
        "");
    /// Remove the "-- Set conf value" lines of the compiled-in string from `messages` before comparing.
    const char * env_header = strstr(messages, "<jemalloc>: malloc_conf #2");
    const char * expected_env_header = strstr(expected, "<jemalloc>: malloc_conf #2");
    REQUIRE(env_header && expected_env_header);
    CHECK_STREQ(env_header, expected_env_header);
    CHECK(strncmp(messages, "<jemalloc>: malloc_conf #1 (string specified via --with-malloc-conf): \"", 71) == 0);
    resetState();
}

TEST(Configuration, AbortConfiguration)
{
    /// An error with `abort_configuration:true` aborts after the source has been processed.
    int signal_number = runInChild(
        []
        {
            je_malloc_message = nullptr;
            int devnull = open("/dev/null", O_WRONLY);
            dup2(devnull, 2);
            parseEnv("abort_conf:true,narenas:x");
        });
    CHECK_EQ(signal_number, SIGABRT);

    /// Experimental keys are tolerated.
    signal_number = runInChild([] { parseEnv("abort_conf:true,experimental_x:1"); });
    CHECK_EQ(signal_number, 0);

    /// `profiling_leak_error` without `profiling_final`.
    signal_number = runInChild([] { parseEnv("abort_conf:true,prof_leak_error:true"); });
    CHECK_EQ(signal_number, SIGABRT);
    parseEnv("prof_leak_error:true");
    CHECK_STREQ(messages, "<jemalloc>: prof_leak_error is set w/o prof_final.\n");
    CHECK(!had_configuration_error);
    resetState();
}

TEST(Configuration, HUGE_PAGE_ALLOCATOR)
{
    parseEnv("hpa:true,hpa_slab_max_alloc:131072");
    CHECK(options.huge_page_allocator);
    CHECK_EQ(options.huge_page_allocator_options.slab_max_alloc, size_t(131072));
    hugePageAllocatorDisableUnsupported();
    CHECK(!options.huge_page_allocator);
    CHECK_STREQ(messages, "<jemalloc>: HPA not supported in the current configuration; disabling.");

    int signal_number = runInChild(
        []
        {
            parseEnv("hpa:true,abort_conf:true");
            hugePageAllocatorDisableUnsupported();
        });
    CHECK_EQ(signal_number, SIGABRT);
    resetState();
}
