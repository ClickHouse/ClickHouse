/// Compares the configuration parser (`mallocConfInit`) with jemalloc's `malloc_conf_init` end-to-end.
///
/// For every case (a value of `MALLOC_CONF` and of the weak globals `je_malloc_conf` / `je_malloc_conf_2_conf_harder`)
/// two child processes are forked: one runs jemalloc's parser (conf_oracle_ref.c), the other runs ours; each prints
/// the messages of the parser (stderr) followed by all option values (stdout) into the same pipe, and the parent
/// compares the outputs byte for byte, together with the exit status (`abort_conf` aborts).
///
/// Both parsers also see the compiled-in configuration string (ClickHouse's Linux `malloc_conf`).

#include <allocator/Conf.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

extern "C" void ref_conf_run(void);

using namespace jemalloc;

namespace
{

#define B(name, value) dprintf(1, "%s=%s\n", name, (value) ? "true" : "false")
#define U(name, value) dprintf(1, "%s=%llu\n", name, static_cast<unsigned long long>(value))
#define I(name, value) dprintf(1, "%s=%lld\n", name, static_cast<long long>(value))
#define S(name, value) dprintf(1, "%s=%s\n", name, (value) != nullptr ? (value) : "(null)")

/// The same format as `dump` in conf_oracle_ref.c.
void dumpOptions(const SizeClassData & sc_data, const unsigned * bin_shard_sizes, const char * readlink_buf)
{
    B("abort", opt.abort);
    B("abort_conf", opt.abort_conf);
    B("confirm_conf", opt.confirm_conf);
    S("junk", opt.junk);
    B("junk_alloc", opt.junk_alloc);
    B("junk_free", opt.junk_free);
    B("trust_madvise", opt.trust_madvise);
    B("cache_oblivious", opt.cache_oblivious);
    U("zero_realloc", unsigned(opt.zero_realloc_action));
    B("disable_large_size_classes", opt.disable_large_size_classes);
    B("utrace", opt.utrace);
    B("xmalloc", opt.xmalloc);
    B("experimental_infallible_new", opt.experimental_infallible_new);
    B("experimental_tcache_gc", opt.experimental_tcache_gc);
    B("zero", opt.zero);
    U("narenas", opt.narenas);
    U("narenas_ratio", opt.narenas_ratio);
    U("debug_double_free_max_scan", opt.debug_double_free_max_scan);
    U("calloc_madvise_threshold", opt.calloc_madvise_threshold);
    B("hpa", opt.hpa);
    U("hpa_slab_max_alloc", opt.hpa_opts.slab_max_alloc);
    U("hpa_hugification_threshold", opt.hpa_opts.hugification_threshold);
    U("hpa_dirty_mult", opt.hpa_opts.dirty_mult);
    B("hpa_deferral_allowed", opt.hpa_opts.deferral_allowed);
    U("hpa_hugify_delay_ms", opt.hpa_opts.hugify_delay_ms);
    B("hpa_hugify_sync", opt.hpa_opts.hugify_sync);
    U("hpa_min_purge_interval_ms", opt.hpa_opts.min_purge_interval_ms);
    I("experimental_hpa_max_purge_nhp", opt.hpa_opts.experimental_max_purge_nhp);
    U("hpa_purge_threshold", opt.hpa_opts.purge_threshold);
    U("hpa_min_purge_delay_ms", opt.hpa_opts.min_purge_delay_ms);
    U("hpa_hugify_style", unsigned(opt.hpa_opts.hugify_style));
    U("hpa_sec_nshards", opt.hpa_sec_opts.nshards);
    U("hpa_sec_max_alloc", opt.hpa_sec_opts.max_alloc);
    U("hpa_sec_max_bytes", opt.hpa_sec_opts.max_bytes);
    U("hpa_sec_batch_fill_extra", opt.hpa_sec_opts.batch_fill_extra);
    B("experimental_hpa_start_huge_if_thp_always", opt.experimental_hpa_start_huge_if_thp_always);
    B("experimental_hpa_enforce_hugify", opt.experimental_hpa_enforce_hugify);
    U("percpu_arena", unsigned(opt.percpu_arena));
    I("dirty_decay_ms", opt.dirty_decay_ms);
    I("muzzy_decay_ms", opt.muzzy_decay_ms);
    U("oversize_threshold", opt.oversize_threshold);
    B("huge_arena_pac_thp", opt.huge_arena_pac_thp);
    U("metadata_thp", unsigned(opt.metadata_thp));
    U("thp", unsigned(opt.thp));
    B("retain", opt.retain);
    S("dss", opt.dss);
    U("dss_prec_default", unsigned(extentDssPrecGet()));
    U("lg_extent_max_active_fit", opt.lg_extent_max_active_fit);
    U("process_madvise_max_batch", opt.process_madvise_max_batch);
    I("mutex_max_spin", opt.mutex_max_spin);
    B("stats_print", opt.stats_print);
    S("stats_print_opts", opt.stats_print_opts);
    I("stats_interval", opt.stats_interval);
    S("stats_interval_opts", opt.stats_interval_opts);
    B("tcache", opt.tcache);
    U("tcache_max", opt.tcache_max);
    U("tcache_nslots_small_min", opt.tcache_nslots_small_min);
    U("tcache_nslots_small_max", opt.tcache_nslots_small_max);
    U("tcache_nslots_large", opt.tcache_nslots_large);
    I("lg_tcache_nslots_mul", opt.lg_tcache_nslots_mul);
    U("tcache_gc_incr_bytes", opt.tcache_gc_incr_bytes);
    U("tcache_gc_delay_bytes", opt.tcache_gc_delay_bytes);
    U("lg_tcache_flush_small_div", opt.lg_tcache_flush_small_div);
    U("lg_tcache_flush_large_div", opt.lg_tcache_flush_large_div);
    dprintf(1, "tcache_ncached_max=");
    for (unsigned i = 0; i < TCACHE_NBINS_MAX; ++i)
        dprintf(1, "%u%s,", unsigned(opt.tcache_ncached_max[i]), opt.tcache_ncached_max_set[i] ? "*" : "");
    dprintf(1, "\n");
    B("background_thread", opt.background_thread);
    U("max_background_threads", opt.max_background_threads);
    B("prof", opt.prof);
    B("prof_active", opt.prof_active);
    B("prof_thread_active_init", opt.prof_thread_active_init);
    U("prof_bt_max", opt.prof_bt_max);
    U("lg_prof_sample", opt.lg_prof_sample);
    I("lg_prof_interval", opt.lg_prof_interval);
    B("prof_gdump", opt.prof_gdump);
    B("prof_final", opt.prof_final);
    B("prof_leak", opt.prof_leak);
    B("prof_leak_error", opt.prof_leak_error);
    B("prof_accum", opt.prof_accum);
    B("prof_pid_namespace", opt.prof_pid_namespace);
    S("prof_prefix", opt.prof_prefix);
    B("prof_sys_thread_name", opt.prof_sys_thread_name);
    B("prof_unbias", opt.prof_unbias);
    B("prof_log", opt.prof_log);
    I("prof_recent_alloc_max", opt.prof_recent_alloc_max);
    B("prof_stats", opt.prof_stats);
    U("prof_time_res", unsigned(opt.prof_time_res));
    U("san_guard_large", opt.san_guard_large);
    U("san_guard_small", opt.san_guard_small);
    I("lg_san_uaf_align", opt.lg_san_uaf_align);
    S("malloc_conf_env_var", opt.malloc_conf_env_var);
    S("malloc_conf_symlink", opt.malloc_conf_symlink);
    S("readlink_buf", readlink_buf);
    B("had_conf_error", had_conf_error);
    dprintf(1, "slab_pgs=");
    for (int i = 0; i < sc_data.nbins; ++i)
        dprintf(1, "%d,", sc_data.sc[i].pgs);
    dprintf(1, "\n");
    dprintf(1, "bin_shards=");
    for (unsigned i = 0; i < SC_NBINS; ++i)
        dprintf(1, "%u,", bin_shard_sizes[i]);
    dprintf(1, "\n");
}

/// The boot steps of `malloc_init_hard_a0_locked` up to the HPA check, as done by `ref_conf_run`.
void ourConfRun()
{
    SizeClassData sc_data{};
    scBoot(sc_data);
    unsigned bin_shard_sizes[SC_NBINS];
    binShardSizesBoot(bin_shard_sizes);
    char readlink_buf[MALLOC_CONF_READLINK_BUF_SIZE];
    readlink_buf[0] = '\0';
    mallocConfInit(sc_data, bin_shard_sizes, readlink_buf);
    hpaDisableUnsupported();
    dumpOptions(sc_data, bin_shard_sizes, readlink_buf);
}

struct Case
{
    std::string env;
    bool has_env = true;
    const char * global = nullptr;
    const char * global_2 = nullptr;
};

std::string runChild(const Case & c, bool reference)
{
    int fds[2];
    REQUIRE(pipe(fds) == 0);
    pid_t pid = fork();
    REQUIRE(pid >= 0);
    if (pid == 0)
    {
        close(fds[0]);
        dup2(fds[1], 1);
        dup2(fds[1], 2);
        close(fds[1]);
        if (c.has_env)
            setenv("MALLOC_CONF", c.env.c_str(), 1);
        else
            unsetenv("MALLOC_CONF");
        je_malloc_conf = c.global;
        je_malloc_conf_2_conf_harder = c.global_2;
        if (reference)
            ref_conf_run();
        else
            ourConfRun();
        _exit(0);
    }
    close(fds[1]);
    std::string out;
    char buf[65536];
    while (true)
    {
        ssize_t n = read(fds[0], buf, sizeof(buf));
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            break;
        out.append(buf, size_t(n));
    }
    close(fds[0]);
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    if (WIFEXITED(status))
        out += "[exit " + std::to_string(WEXITSTATUS(status)) + "]\n";
    else if (WIFSIGNALED(status))
        out += "[signal " + std::to_string(WTERMSIG(status)) + "]\n";
    return out;
}

/// Print the first differing line of two outputs.
void reportDifference(const Case & c, const std::string & expected, const std::string & actual)
{
    std::fprintf(stderr, "MISMATCH for MALLOC_CONF=%s%s%s global=%s global_2=%s\n", c.has_env ? "\"" : "",
        c.has_env ? c.env.substr(0, 200).c_str() : "(unset)", c.has_env ? "\"" : "", c.global ? c.global : "(null)",
        c.global_2 ? c.global_2 : "(null)");
    size_t pos_e = 0;
    size_t pos_a = 0;
    while (pos_e < expected.size() || pos_a < actual.size())
    {
        size_t end_e = expected.find('\n', pos_e);
        size_t end_a = actual.find('\n', pos_a);
        std::string line_e = pos_e < expected.size() ? expected.substr(pos_e, end_e - pos_e) : "<eof>";
        std::string line_a = pos_a < actual.size() ? actual.substr(pos_a, end_a - pos_a) : "<eof>";
        if (line_e != line_a)
        {
            std::fprintf(stderr, "  jemalloc: %s\n  ours:     %s\n", line_e.substr(0, 300).c_str(), line_a.substr(0, 300).c_str());
            return;
        }
        pos_e = end_e == std::string::npos ? expected.size() : end_e + 1;
        pos_a = end_a == std::string::npos ? actual.size() : end_a + 1;
    }
}

size_t runCases(const std::vector<Case> & cases)
{
    size_t mismatches = 0;
    for (const auto & c : cases)
    {
        std::string expected = runChild(c, true);
        std::string actual = runChild(c, false);
        if (expected != actual)
        {
            reportDifference(c, expected, actual);
            ++mismatches;
        }
    }
    return mismatches;
}

std::vector<Case> envCases(const std::vector<std::string> & strings)
{
    std::vector<Case> cases;
    for (const auto & s : strings)
        cases.push_back(Case{s});
    return cases;
}

/// `key:value` for every value.
std::vector<std::string> keyValues(const char * key, const std::vector<std::string> & values)
{
    std::vector<std::string> result;
    for (const auto & v : values)
        result.push_back(std::string(key) + ":" + v);
    return result;
}

const std::vector<std::string> bool_values = {"true", "false", "", "1", "0", "TRUE", "tru", "truex", " true", "yes"};

const std::vector<std::string> number_values = {
    "0",
    "1",
    "2",
    "-1",
    "-2",
    "08",
    "010",
    "0x10",
    "0X1f",
    "0x",
    "00",
    " 5",
    "\t7",
    "+5",
    "-0",
    "5x",
    "5 ",
    "",
    "x",
    "1000",
    "2048",
    "2049",
    "65536",
    "4294967295",
    "4294967296",
    "9223372036854775807",
    "9223372036854775808",
    "18446744072000",
    "18446744072001",
    "18446744073709551615",
    "18446744073709551616",
    "99999999999999999999999",
    "0xffffffffffffffff",
    "0x10000000000000000",
    "-9223372036854775808",
    "-9223372036854775809",
    "123456789",
};

}

TEST(ConfOracle, Defaults)
{
    std::vector<Case> cases;
    cases.push_back(Case{"", false});
    cases.push_back(Case{""});
    cases.push_back(Case{"", false, "", ""});
    CHECK_EQ(runCases(cases), size_t(0));
}

TEST(ConfOracle, Syntax)
{
    CHECK_EQ(
        runCases(envCases({
            "abc",
            "abort",
            "abort:",
            "abort:true,",
            "abort:true,,",
            ",",
            ",abort:true",
            ":",
            "::",
            ":x",
            ":always",
            "a!b:c",
            "abort!:true",
            "ab cd:1",
            "abort:true,narenas",
            "abort:true,narenas:2,xyz",
            "narenas:2,narenas:5",
            "narenas:5,narenas:2",
            "narenas:2:3",
            "prof_prefix:a:b,narenas:3",
            "Abort:true",
            "ABORT:true",
            "unknown_key:1",
            "utrace:true",
            "xmalloc:true",
            "log:abc",
            "experimental_infallible_new:true",
            "experimental_foo:1",
            "experimental:1",
            "experimental_hpa_max_purge_nhp:x",
            "hpa_sec_bytes_after_flush:1",
            "hpa_sec_bytes_after_flushX:1",
            "lg_san_uaf_align:12",
            "lg_san_uaf_align:-1",
            std::string(5000, 'a'),
            std::string(5000, 'a') + ":1",
            "abort:true," + std::string(3000, 'x') + ":" + std::string(3000, 'y'),
            "a\xff:1",
            "\x01",
        })),
        size_t(0));
}

TEST(ConfOracle, Bools)
{
    std::vector<std::string> strings;
    for (const char * key : {"abort", "cache_oblivious", "trust_madvise", "experimental_hpa_start_huge_if_thp_always",
             "experimental_hpa_enforce_hugify", "huge_arena_pac_thp", "retain", "stats_print", "zero", "experimental_tcache_gc",
             "tcache", "background_thread", "hpa_hugify_sync", "prof", "prof_active", "prof_thread_active_init", "prof_accum",
             "prof_gdump", "prof_final", "prof_leak", "prof_log", "prof_pid_namespace", "prof_stats", "prof_sys_thread_name",
             "prof_unbias", "disable_large_size_classes"})
    {
        for (const auto & s : keyValues(key, bool_values))
            strings.push_back(s);
    }
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, Numbers)
{
    std::vector<std::string> strings;
    for (const char * key : {"narenas", "mutex_max_spin", "dirty_decay_ms", "muzzy_decay_ms", "process_madvise_max_batch",
             "stats_interval", "tcache_max", "lg_tcache_max", "lg_tcache_nslots_mul", "tcache_nslots_small_min",
             "tcache_nslots_small_max", "tcache_nslots_large", "tcache_gc_incr_bytes", "tcache_gc_delay_bytes",
             "lg_tcache_flush_small_div", "lg_tcache_flush_large_div", "debug_double_free_max_scan", "calloc_madvise_threshold",
             "oversize_threshold", "lg_extent_max_active_fit", "max_background_threads", "hpa_slab_max_alloc",
             "hpa_hugification_threshold", "hpa_hugify_delay_ms", "hpa_min_purge_interval_ms", "experimental_hpa_max_purge_nhp",
             "hpa_purge_threshold", "hpa_min_purge_delay_ms", "hpa_sec_nshards", "hpa_sec_max_alloc", "hpa_sec_max_bytes",
             "hpa_sec_batch_fill_extra", "lg_prof_sample", "prof_bt_max", "lg_prof_interval", "prof_recent_alloc_max",
             "san_guard_small", "san_guard_large"})
    {
        for (const auto & s : keyValues(key, number_values))
            strings.push_back(s);
    }
    for (const auto & s : std::vector<std::string>{
             "narenas:default", "narenas:defaul", "narenas:3,narenas:default", "tcache_max:32768", "tcache_max:524288",
             "tcache_max:524289", "lg_tcache_max:15", "lg_tcache_max:19", "lg_tcache_max:20", "lg_tcache_max:64",
             "max_background_threads:4095", "max_background_threads:4096", "max_background_threads:4097",
             "max_background_threads:10,max_background_threads:20", "max_background_threads:20,max_background_threads:10",
             "hpa_slab_max_alloc:65535", "hpa_slab_max_alloc:536870913", "hpa_sec_max_alloc:524289",
             "hpa_sec_batch_fill_extra:8192", "hpa_sec_batch_fill_extra:8193", "hpa_sec_max_bytes:262143",
             "hpa_sec_max_bytes:262145", "oversize_threshold:0x7000000000000000", "oversize_threshold:0x7000000000000001",
             "calloc_madvise_threshold:8388608", "lg_extent_max_active_fit:64", "lg_extent_max_active_fit:65",
             "lg_prof_sample:63", "lg_prof_sample:64", "lg_prof_interval:63", "lg_prof_interval:64",
             "prof_recent_alloc_max:-1", "mutex_max_spin:-1", "dirty_decay_ms:-1,muzzy_decay_ms:-1"})
        strings.push_back(s);
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, Enums)
{
    std::vector<std::string> strings;
    std::vector<std::string> enum_values = {"", "a", "d", "p", "s", "x", "n", "l", "e", "ph", "pe", "al", "default", "disabled",
        "auto", "always", "never", "not supported", "not", "primary", "secondary", "N/A", "percpu", "phycpu", "eager", "lazy",
        "none", "high", "alloc", "free", "abort", "true", "false", "disabledx", "phycpux"};
    for (const char * key : {"metadata_thp", "metadata_th", "m", "me", "", "dss", "ds", "d", "percpu_arena", "percpu", "p", "per",
             "hpa_hugify_style", "hpa_hugify_s", "hpa_", "hpa_h", "thp", "th", "zero_realloc", "zero_reallo", "junk", "jun",
             "prof_time_resolution", "prof_time_res"})
    {
        for (const auto & s : keyValues(key, enum_values))
            strings.push_back(s);
    }
    for (const auto & s : std::vector<std::string>{"dss:primary,dss:disabled", "dss:primary,dss:x", "percpu_arena:phycpu,p:d",
             "junk:alloc,junk:free", "junk:true,junk:x", "zero_realloc:abort,zero_realloc:free", "metadata_thp:always,m:x",
             "thp:always,thp:never", "prof_time_resolution:high,prof_time_resolution:default"})
        strings.push_back(s);
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, FixedPoint)
{
    std::vector<std::string> strings;
    std::vector<std::string> values = {"", "0", "1", "2.5", "0.0", ".5", "0.25", "1.0", "1.00001", "1.5", "65535", "65536",
        "65535.99999999999999", "3.14159265358979323846", "1.", ".", "abc", "-1", "-0.5", "1e3", " 1", "1 ", "0x1",
        "0.000001", "0.99999999999999999999"};
    for (const char * key : {"narenas_ratio", "hpa_hugification_threshold_ratio", "hpa_purge_threshold_ratio", "hpa_dirty_mult"})
    {
        for (const auto & s : keyValues(key, values))
            strings.push_back(s);
    }
    strings.push_back("hpa_dirty_mult:-1");
    strings.push_back("hpa_dirty_mult:-1,hpa_dirty_mult:0.5");
    strings.push_back("hpa_hugification_threshold:65536,hpa_hugification_threshold_ratio:0.5");
    strings.push_back("hpa_purge_threshold_ratio:0.5,hpa_purge_threshold:131072");
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, MultiSettings)
{
    std::vector<std::string> values = {"default", "", "1-2", "1-2:", "1-2:3", "1-2:3|", "1-2:3|4", "1-2:3|4-5", "1-2:3x",
        "1-4096:1", "1-100:3|200-300:5", "1-100:3|200-300:5|", "0-0:0", "0-0:1", "1-16:-1", "1-16:0", "1-16:2",
        "1-160:16", "1-160:64", "1-160:65", "8-8:2|16-16:3", "100000-200000:2", "200-100:2", "1-18446744073709551615:2",
        "x", "-1-2:3", "1--2:3", "0x10-0x20:0x3", "010-020:010", " 1- 2: 3", "1-2:3 |4-5:6", "1-2:3||4-5:6",
        "1-262144:100", "1-524288:100", "1-524289:100", "1-1048576:9000", "8-8:8191", "8-8:8192", "8-8:65536",
        "0-1000000000:5", "1-2:99999999999999999999", "16-16:1|16-16:2", "4096-8192:1|0-16:0"};
    std::vector<std::string> strings;
    for (const char * key : {"slab_sizes", "bin_shards", "tcache_ncached_max"})
    {
        for (const auto & s : keyValues(key, values))
            strings.push_back(s);
    }
    strings.push_back("slab_sizes:1-100:3,slab_sizes:default");
    strings.push_back("slab_sizes:1-100:3,slab_sizes:200-300:4");
    strings.push_back("bin_shards:1-100:3,bin_shards:50-60:4");
    strings.push_back("tcache_ncached_max:1-100:3,tcache_ncached_max:50-60:4");
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, StringsAndStatsOpts)
{
    std::vector<std::string> strings = {"stats_print_opts:Jgmd", "stats_print_opts:JJxx", "stats_print_opts:zzz",
        "stats_print_opts:Jgmdablxeh", "stats_print_opts:Jgmdablxehz", "stats_print_opts:", "stats_print_opts:a,stats_print_opts:ab",
        "stats_print_opts:hgfedcbaJ", "stats_interval_opts:mdx", "stats_interval_opts:mdx,stats_interval_opts:J",
        "stats_print_opts:J,stats_interval_opts:g", "prof_prefix:", "prof_prefix:/tmp/abc", "prof_prefix:a b c",
        "prof_prefix:" + std::string(4095, 'p'), "prof_prefix:" + std::string(4096, 'p'), "prof_prefix:" + std::string(4097, 'p'),
        "prof_prefix:" + std::string(5000, 'p') + ",narenas:3", "prof_prefix:x:y:z"};
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, ConfirmAndAbort)
{
    std::vector<std::string> strings = {
        "confirm_conf:true",
        "confirm_conf:true,narenas:3,abort:x,narenas:x,dss:p",
        "confirm_conf:false",
        "confirm_conf:x",
        "confirm_conf:true,confirm_conf:false",
        "narenas:3,confirm_conf:true",
        "confirm_conf:true,prof_prefix:" + std::string(5000, 'q'),
        "confirm_conf:true,abc",
        "confirm_conf:true,unknown:1,metadata_thp:a,bin_shards:x",
        "abort_conf:true",
        "abort_conf:true,narenas:x",
        "narenas:x,abort_conf:true",
        "abort_conf:true,experimental_foo:1",
        "abort_conf:true,hpa_sec_bytes_after_flush:1",
        "abort_conf:true,abc",
        "abort_conf:true,abort:true,",
        "abort_conf:true,narenas:3,abort_conf:false,narenas:x",
        "abort_conf:true,confirm_conf:true,narenas:x",
        "prof_leak_error:true",
        "prof_leak_error:true,prof_final:true",
        "prof_leak_error:true,abort_conf:true",
        "prof_leak_error:true,prof_final:true,abort_conf:true",
        "debug_double_free_max_scan:100",
        "percpu_arena:disabled,background_thread:false,prof:false,muzzy_decay_ms:1,dirty_decay_ms:2,oversize_threshold:3",
    };
    /// HPA is not implemented: `hpa:true` is reported as unsupported and disabled. jemalloc behaves the same only where
    /// it does not support HPA itself, e.g. when the huge page is larger than HUGEPAGE_MAX_EXPECTED_SIZE (16 MiB),
    /// as on aarch64 with 64 KiB pages.
    if constexpr (HUGEPAGE > (size_t(16) << 20))
    {
        strings.push_back("hpa:true");
        strings.push_back("hpa:true,abort_conf:true");
        strings.push_back("hpa:true,hpa:false");
        strings.push_back("hpa:true,hpa_hugify_sync:true");
    }
    CHECK_EQ(runCases(envCases(strings)), size_t(0));
}

TEST(ConfOracle, Sources)
{
    std::vector<Case> cases;
    cases.push_back(Case{"dirty_decay_ms:500", true, "dirty_decay_ms:1000,muzzy_decay_ms:2000", "dirty_decay_ms:1234"});
    cases.push_back(Case{"", false, "dirty_decay_ms:1000,muzzy_decay_ms:2000", nullptr});
    cases.push_back(Case{"narenas:3", true, nullptr, "narenas:5"});
    cases.push_back(Case{"narenas:3", true, "narenas:x", nullptr});
    cases.push_back(Case{"confirm_conf:true", true, "narenas:7", "narenas:9"});
    cases.push_back(Case{"", false, "abort_conf:true", "narenas:x"});
    cases.push_back(Case{"narenas:x", true, nullptr, "abort_conf:true"});
    cases.push_back(Case{"abort_conf:true", true, "narenas:x", nullptr});
    cases.push_back(Case{"", false, nullptr, "confirm_conf:true"});
    cases.push_back(Case{"", false, "confirm_conf:true", "confirm_conf:false"});
    cases.push_back(Case{"a!", true, "b!", "c!"});
    cases.push_back(Case{"slab_sizes:1-100:3", true, "slab_sizes:1-4096:2", "bin_shards:1-100:4"});
    CHECK_EQ(runCases(cases), size_t(0));
}
