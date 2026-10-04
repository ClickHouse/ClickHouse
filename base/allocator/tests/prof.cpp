/// Heap profiling: the sampling wait values (pinned from jemalloc's C formula), the unbiasing tables, the tdata /
/// tctx / gctx life cycles, `prof.reset`, the recent allocation records, the hooks and the exact heap dump format.
///
/// The allocator is initialized with `lg_prof_sample:0` and a deterministic backtrace hook; sampled allocations are
/// made through the same inline functions the front-end uses (`profAllocPrep`, `profMalloc`, `profFree`).

#include <allocator/Ctl.h>
#include <allocator/Frontend.h>
#include <allocator/Init.h>
#include <allocator/Options.h>
#include <allocator/Prof.h>
#include <allocator/ProfHooks.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <vector>

using namespace jemalloc;

namespace
{

void init()
{
    static bool initialized = false;
    if (initialized)
        return;
    initialized = true;
    setenv("MALLOC_CONF", "background_thread:false,prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:0", 1);
    REQUIRE(!mallocInit());
    REQUIRE(opt.prof);
    REQUIRE(lg_prof_sample == 0);
}

/// The backtrace returned by the test hook.
void * test_bt[8];
unsigned test_bt_len = 0;

void testBacktraceHook(void ** vec, unsigned * len, unsigned max_len)
{
    REQUIRE(test_bt_len <= max_len);
    for (unsigned i = 0; i < test_bt_len; ++i)
        vec[i] = test_bt[i];
    *len = test_bt_len;
}

void setBacktrace(std::initializer_list<uintptr_t> frames)
{
    test_bt_len = 0;
    for (uintptr_t frame : frames)
        test_bt[test_bt_len++] = reinterpret_cast<void *>(frame);
}

void installBacktraceHook()
{
    ThreadState & tsd = ThreadState::fetch();
    ProfBacktraceHook hook = testBacktraceHook;
    ProfBacktraceHook old_hook = nullptr;
    size_t old_size = sizeof(old_hook);
    REQUIRE(ctlByName(tsd, "experimental.hooks.prof_backtrace", &old_hook, &old_size, &hook, sizeof(hook)) == 0);
    REQUIRE(old_hook == testBacktraceHook || old_hook == profBacktraceImpl);
}

/// A sampled large allocation made like `imalloc_body` does (always sampled: `sample_event = true`). Small sizes
/// would need the promotion of `imalloc_sample`, which is not done here.
void * sampledMalloc(size_t size)
{
    ThreadState & tsd = ThreadState::fetch();
    /// `imalloc_sample` of a large size: page-aligned, so that the free path can recognize sampled objects.
    size_t usize = sz::sa2u(size, PROF_SAMPLE_ALIGNMENT);
    ProfThreadContext * tctx = profAllocPrep(tsd, profActiveGetUnlocked(), true);
    REQUIRE(profTctxIsValid(tctx));
    void * ptr = ipalloc(tsd, usize, PROF_SAMPLE_ALIGNMENT, false);
    REQUIRE(ptr != nullptr);
    profMalloc(tsd, ptr, size, usize, nullptr, tctx);
    return ptr;
}

void sampledFree(void * ptr)
{
    ThreadState & tsd = ThreadState::fetch();
    size_t usize = isalloc(&tsd, ptr);
    profFree(tsd, ptr, usize, nullptr);
    idalloc(tsd, ptr);
}

void appendCallback(void * opaque, const char * s)
{
    static_cast<std::string *>(opaque)->append(s);
}

/// `prof_dump` without the file: the output of `profDumpImpl`.
std::string dumpToString()
{
    ThreadState & tsd = ThreadState::fetch();
    std::string out;
    ProfThreadData * tdata = profTdataGet(tsd, true);
    REQUIRE(tdata != nullptr);
    preReentrancy(tsd, nullptr);
    prof_dump_mtx.lock(&tsd);
    profDumpImpl(tsd, appendCallback, &out, tdata, false);
    prof_dump_mtx.unlock(&tsd);
    postReentrancy(tsd);
    return out;
}

/// Replaces the age of `f:` records with N and splits off the `frag_util:` lines (checking their format).
std::string normalizeDump(const std::string & dump, size_t * frag_util_lines)
{
    std::string result;
    *frag_util_lines = 0;
    size_t pos = 0;
    while (pos < dump.size())
    {
        size_t end = dump.find('\n', pos);
        REQUIRE(end != std::string::npos);
        std::string line = dump.substr(pos, end - pos);
        pos = end + 1;
        if (line.starts_with("frag_util: "))
        {
            unsigned a, b, e, f;
            size_t c, d, g, h, i;
            int n = 0;
            CHECK_EQ(std::sscanf(line.c_str(), "frag_util: %u %u %zu %zu %u %u %zu %zu %zu%n", &a, &b, &c, &d, &e, &f, &g, &h, &i, &n), 9);
            CHECK_EQ(size_t(n), line.size());
            CHECK_GT(g, size_t(0));
            ++*frag_util_lines;
            continue;
        }
        CHECK_EQ(*frag_util_lines, size_t(0)); /// `frag_util:` lines are last.
        if (line.starts_with("  f: "))
        {
            size_t space = line.find(' ', 5);
            line = "  f: N" + line.substr(space);
        }
        result += line;
        result += '\n';
    }
    return result;
}

std::string fmt(const char * format, ...) __attribute__((format(printf, 1, 2)));
std::string fmt(const char * format, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, format);
    std::vsnprintf(buf, sizeof(buf), format, ap);
    va_end(ap);
    return buf;
}

/// The unbiased size of one sample: `prof_unbiased_sz` uses the size of the class (not the usize, which differs with
/// `disable_large_size_classes`); with a rate of 1 byte the unbiasing is the identity otherwise.
size_t unbiasedSize(void * ptr)
{
    return sz::indexToSizeUnsafe(sz::sizeToIndex(isalloc(&ThreadState::fetch(), ptr)));
}

uint64_t mainThrUid()
{
    ThreadState & tsd = ThreadState::fetch();
    ProfThreadData * tdata = profTdataGet(tsd, true);
    REQUIRE(tdata != nullptr);
    return tdata->thr_uid;
}

std::string fRecord(void * ptr, size_t size, uint64_t thr_uid)
{
    ThreadState & tsd = ThreadState::fetch();
    size_t usize = isalloc(&tsd, ptr);
    Extent * edata = arena_emap_global.edataLookup(&tsd, ptr);
    return fmt("  f: N %zu %zu %u %u %llu\n", size, usize, unsigned(sz::sizeToIndex(usize)), edata->arenaInd(), (unsigned long long)thr_uid);
}

}

/// The waits of `prof_sample_new_event_wait` for fixed PRNG states, computed by the C formula.
TEST(Prof, SampleNewEventWait)
{
    init();
    size_t saved_lg = lg_prof_sample;
    static ThreadState tsd;

    lg_prof_sample = 19;
    tsd.prng_state = 42;
    const uint64_t expected_19[] = {296343, 780978, 463837, 241909, 202085};
    for (uint64_t expected : expected_19)
        CHECK_EQ(profSampleNewEventWait(tsd), expected);

    lg_prof_sample = 10;
    tsd.prng_state = 0xfffff7ff0000ULL;
    const uint64_t expected_10[] = {1620, 2153, 463, 226, 1458};
    for (uint64_t expected : expected_10)
        CHECK_EQ(profSamplePostponedEventWait(tsd), expected);

    /// lg_prof_sample == 0: sample every allocation; no PRNG draw.
    lg_prof_sample = 0;
    tsd.prng_state = 7;
    CHECK_EQ(profSampleNewEventWait(tsd), uint64_t(1));
    CHECK_EQ(tsd.prng_state, uint64_t(7));

    lg_prof_sample = saved_lg;
}

TEST(Prof, UnbiasMap)
{
    init();
    size_t saved_lg = lg_prof_sample;
    lg_prof_sample = 19;
    profUnbiasMapInit();
    szind_t ind = sz::sizeToIndex(4096);
    CHECK_EQ(prof_unbiased_sz[ind], size_t(526339));
    CHECK_EQ(prof_shifted_unbiased_cnt[ind], size_t(1028));
    lg_prof_sample = saved_lg;
    profUnbiasMapInit();
    /// With a rate of 1 byte the unbiasing is the identity (for sizes >= 8).
    CHECK_EQ(prof_unbiased_sz[ind], size_t(4096));
    CHECK_EQ(prof_shifted_unbiased_cnt[ind], size_t(8));
}

TEST(Prof, CtlErrors)
{
    init();
    ThreadState & tsd = ThreadState::fetch();
    size_t value = 0;
    size_t size = sizeof(value);
    const char * filename = "x";
    CHECK_EQ(ctlByName(tsd, "prof.dump", &value, &size, &filename, sizeof(filename)), EPERM);
    CHECK_EQ(ctlByName(tsd, "prof.reset", nullptr, nullptr, &value, 1), EINVAL);
    CHECK_EQ(ctlByName(tsd, "prof.log_start", nullptr, nullptr, nullptr, 0), ENOENT);
    CHECK_EQ(ctlByName(tsd, "prof.log_stop", nullptr, nullptr, nullptr, 0), ENOENT);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_sample", nullptr, nullptr, nullptr, 0), EINVAL);
    ProfBacktraceHook null_hook = nullptr;
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_backtrace", nullptr, nullptr, &null_hook, sizeof(null_hook)), EINVAL);
    CHECK_EQ(ctlByName(tsd, "prof.stats.bins.0.live", nullptr, nullptr, nullptr, 0), ENOENT);

    uint64_t interval = 1;
    size = sizeof(interval);
    CHECK_EQ(ctlByName(tsd, "prof.interval", &interval, &size, nullptr, 0), 0);
    CHECK_EQ(interval, uint64_t(0));
    size = sizeof(value);
    CHECK_EQ(ctlByName(tsd, "prof.lg_sample", &value, &size, nullptr, 0), 0);
    CHECK_EQ(value, size_t(0));

    bool active = false;
    size_t bool_size = sizeof(active);
    CHECK_EQ(ctlByName(tsd, "prof.active", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(ctlByName(tsd, "thread.prof.active", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(ctlByName(tsd, "prof.thread_active_init", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(ctlByName(tsd, "prof.gdump", &active, &bool_size, nullptr, 0), 0);
    CHECK(!active);
}

TEST(Prof, ThreadName)
{
    init();
    ThreadState & tsd = ThreadState::fetch();
    const char * bad = "a\x01";
    CHECK_EQ(ctlByName(tsd, "thread.prof.name", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);
    const char * longname = "0123456789abcdefghij";
    CHECK_EQ(ctlByName(tsd, "thread.prof.name", nullptr, nullptr, &longname, sizeof(longname)), 0);
    const char * name = nullptr;
    size_t size = sizeof(name);
    CHECK_EQ(ctlByName(tsd, "thread.prof.name", &name, &size, nullptr, 0), 0);
    CHECK_STREQ(name, "0123456789abcde");
    /// Reading and writing at the same time.
    CHECK_EQ(ctlByName(tsd, "thread.prof.name", &name, &size, &longname, sizeof(longname)), EPERM);
    const char * good = "prof test";
    CHECK_EQ(ctlByName(tsd, "thread.prof.name", nullptr, nullptr, &good, sizeof(good)), 0);
}

/// The life cycle of the structures and the exact dump format; the first gctx of the process gets the lock 1023.
TEST(Prof, DumpFormatAndLifecycle)
{
    init();
    installBacktraceHook();
    ThreadState & tsd = ThreadState::fetch();
    uint64_t uid = mainThrUid();
    ProfThreadData * tdata = profTdataGet(tsd, false);
    CHECK(tdata->lock == &tdata_locks[uid % PROF_NTDATA_LOCKS]);
    CHECK_EQ(profBtCount(), size_t(0));
    size_t tdatas_before = profTdataCount();

    setBacktrace({0x201});
    size_t size_a = (4 << 20) + 5;
    void * a = sampledMalloc(size_a);
    setBacktrace({0x102, 0x3000});
    size_t size_b = (3 << 20) + 17;
    void * b = sampledMalloc(size_b);
    /// A second allocation with the same backtrace (same tctx).
    void * b2 = sampledMalloc(size_b);

    CHECK_EQ(profBtCount(), size_t(2));
    CHECK_EQ(tdata->bt2tctx.count(), size_t(2));
    CHECK_EQ(profTdataCount(), tdatas_before);

    Extent * edata_a = arena_emap_global.edataLookup(&tsd, a);
    ProfThreadContext * tctx_a = edata_a->profTctx();
    REQUIRE(profTctxIsValid(tctx_a));
    CHECK(tctx_a->gctx->lock == &gctx_locks[PROF_NCTX_LOCKS - 1]);
    ProfThreadContext * tctx_b = arena_emap_global.edataLookup(&tsd, b)->profTctx();
    CHECK(tctx_b->gctx->lock == &gctx_locks[0]);
    CHECK_EQ(tctx_a->tctx_uid, uint64_t(0));
    CHECK_EQ(tctx_b->tctx_uid, uint64_t(1));
    CHECK_EQ(tctx_b->cnts.curobjs, uint64_t(2));

    size_t usize_a = isalloc(&tsd, a);
    size_t usize_b = isalloc(&tsd, b);
    size_t frag_util_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &frag_util_lines);
    CHECK_GT(frag_util_lines, size_t(0));

    auto expected_dump = [&](size_t bytes_a, size_t bytes_b)
    {
        std::string block_a = fmt("@ 0x201\n  t*: 1: %zu [0: 0]\n  t%llu: 1: %zu [0: 0]\n", bytes_a, (unsigned long long)uid, bytes_a)
            + fRecord(a, size_a, uid);
        std::string block_b
            = fmt("@ 0x102 0x3000\n  t*: 2: %zu [0: 0]\n  t%llu: 2: %zu [0: 0]\n", 2 * bytes_b, (unsigned long long)uid, 2 * bytes_b)
            + fRecord(b, size_b, uid) + fRecord(b2, size_b, uid);
        /// The blocks are ordered by `memcmp` of the raw program counters: 0x201 < 0x102 in little-endian byte order.
        std::string blocks = config::big_endian ? block_b + block_a : block_a + block_b;
        return fmt(
                   "heap_v2/1\n  t*: 3: %zu [0: 0]\n  t%llu: 3: %zu [0: 0] prof test\n",
                   bytes_a + 2 * bytes_b,
                   (unsigned long long)uid,
                   bytes_a + 2 * bytes_b)
            + blocks;
    };
    std::string expected = expected_dump(unbiasedSize(a), unbiasedSize(b));
    CHECK_STREQ(dump.c_str(), expected.c_str());

    /// `prof_unbias:false` prints the raw counters (the usizes).
    opt.prof_unbias = false;
    std::string raw = normalizeDump(dumpToString(), &frag_util_lines);
    opt.prof_unbias = true;
    expected = expected_dump(usize_a, usize_b);
    CHECK_STREQ(raw.c_str(), expected.c_str());

    /// Freeing destroys the tctx and the gctx (and nothing is left in the dump).
    sampledFree(a);
    CHECK_EQ(profBtCount(), size_t(1));
    CHECK_EQ(tdata->bt2tctx.count(), size_t(1));
    sampledFree(b);
    CHECK_EQ(profBtCount(), size_t(1));
    sampledFree(b2);
    CHECK_EQ(profBtCount(), size_t(0));
    CHECK_EQ(tdata->bt2tctx.count(), size_t(0));

    dump = normalizeDump(dumpToString(), &frag_util_lines);
    expected = fmt("heap_v2/1\n  t*: 0: 0 [0: 0]\n  t%llu: 0: 0 [0: 0] prof test\n", (unsigned long long)uid);
    CHECK_STREQ(dump.c_str(), expected.c_str());
}

namespace
{

struct ThreadResult
{
    uint64_t thr_uid = 0;
    void * ptr = nullptr;
    bool keep = false;
};

void * threadBody(void * arg)
{
    auto * result = static_cast<ThreadResult *>(arg);
    ThreadState & tsd = ThreadState::fetch();
    ProfThreadData * tdata = profTdataGet(tsd, true);
    REQUIRE(tdata != nullptr);
    result->thr_uid = tdata->thr_uid;
    if (result->keep)
    {
        setBacktrace({0x7777});
        result->ptr = sampledMalloc(1 << 20);
    }
    return nullptr;
}

}

/// A tdata is destroyed at thread exit unless it still has tctxs; then it is detached and destroyed with its last
/// tctx (it is still dumped while detached).
TEST(Prof, ThreadExit)
{
    init();
    installBacktraceHook();
    size_t tdatas_before = profTdataCount();

    ThreadResult first;
    pthread_t thread;
    REQUIRE(pthread_create(&thread, nullptr, threadBody, &first) == 0);
    pthread_join(thread, nullptr);
    CHECK_EQ(profTdataCount(), tdatas_before);

    ThreadResult second;
    second.keep = true;
    REQUIRE(pthread_create(&thread, nullptr, threadBody, &second) == 0);
    pthread_join(thread, nullptr);
    CHECK_EQ(second.thr_uid, first.thr_uid + 1);
    CHECK_EQ(profTdataCount(), tdatas_before + 1);

    size_t frag_util_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &frag_util_lines);
    CHECK(dump.find(fmt("\n  t%llu: 1: %zu [0: 0]\n", (unsigned long long)second.thr_uid, unbiasedSize(second.ptr))) != std::string::npos);
    CHECK(dump.find("@ 0x7777\n") != std::string::npos);

    sampledFree(second.ptr);
    CHECK_EQ(profTdataCount(), tdatas_before);
    CHECK_EQ(profBtCount(), size_t(0));
}

/// `prof.reset` expires all tdatas: their samples vanish from the dumps (but their `f:` records stay listed under
/// their gctx); the thread gets a new tdata with the same uid and the next discriminator.
TEST(Prof, Reset)
{
    init();
    installBacktraceHook();
    ThreadState & tsd = ThreadState::fetch();
    uint64_t uid = mainThrUid();
    ProfThreadData * old_tdata = profTdataGet(tsd, false);
    uint64_t old_discrim = old_tdata->thr_discrim;
    size_t tdatas_before = profTdataCount();

    setBacktrace({0x4444});
    void * x = sampledMalloc(1 << 20);

    size_t lg = 100;
    CHECK_EQ(ctlByName(tsd, "prof.reset", nullptr, nullptr, &lg, sizeof(lg)), 0);
    CHECK_EQ(lg_prof_sample, size_t(63));
    lg = 0;
    CHECK_EQ(ctlByName(tsd, "prof.reset", nullptr, nullptr, &lg, sizeof(lg)), 0);
    CHECK_EQ(lg_prof_sample, size_t(0));
    CHECK(old_tdata->expired);

    ProfThreadData * new_tdata = profTdataGet(tsd, true);
    CHECK(new_tdata != old_tdata);
    CHECK_EQ(new_tdata->thr_uid, uid);
    CHECK_EQ(new_tdata->thr_discrim, old_discrim + 1);
    CHECK_STREQ(new_tdata->thread_name, "prof test");
    CHECK_EQ(profTdataCount(), tdatas_before + 1);

    /// The sample of the expired tdata is not counted; its gctx has no counters and is skipped.
    size_t frag_util_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &frag_util_lines);
    std::string expected = fmt("heap_v2/1\n  t*: 0: 0 [0: 0]\n  t%llu: 0: 0 [0: 0] prof test\n", (unsigned long long)uid);
    CHECK_STREQ(dump.c_str(), expected.c_str());

    /// A new sample with the same backtrace: the gctx is dumped again, with the `f:` record of the old sample too.
    void * y = sampledMalloc(1 << 20);
    dump = normalizeDump(dumpToString(), &frag_util_lines);
    size_t bytes = unbiasedSize(y);
    expected = fmt(
                   "heap_v2/1\n  t*: 1: %zu [0: 0]\n  t%llu: 1: %zu [0: 0] prof test\n@ 0x4444\n  t*: 1: %zu [0: 0]\n"
                   "  t%llu: 1: %zu [0: 0]\n",
                   bytes,
                   (unsigned long long)uid,
                   bytes,
                   bytes,
                   (unsigned long long)uid,
                   bytes)
        + fRecord(x, 1 << 20, uid) + fRecord(y, 1 << 20, uid);
    CHECK_STREQ(dump.c_str(), expected.c_str());

    sampledFree(x);
    CHECK_EQ(profTdataCount(), tdatas_before);
    sampledFree(y);
    CHECK_EQ(profBtCount(), size_t(0));
}

namespace
{

const void * hook_ptr = nullptr;
size_t hook_size = 0;
size_t hook_usize = 0;
unsigned hook_bt_len = 0;
void * hook_bt0 = nullptr;
const void * hook_free_ptr = nullptr;
size_t hook_free_usize = 0;
std::string hook_dump_filename;

void testSampleHook(const void * ptr, size_t size, void ** backtrace, unsigned backtrace_length, size_t usize)
{
    hook_ptr = ptr;
    hook_size = size;
    hook_usize = usize;
    hook_bt_len = backtrace_length;
    hook_bt0 = backtrace[0];
    /// Inside the hook, the thread is reentrant.
    CHECK_GT(ThreadState::fetch().reentrancyLevel(), int8_t(0));
}

void testSampleFreeHook(const void * ptr, size_t usize)
{
    hook_free_ptr = ptr;
    hook_free_usize = usize;
}

void testDumpHook(const char * filename)
{
    hook_dump_filename = filename;
}

}

TEST(Prof, Hooks)
{
    init();
    installBacktraceHook();
    ThreadState & tsd = ThreadState::fetch();

    ProfSampleHook sample_hook = testSampleHook;
    ProfSampleFreeHook free_hook = testSampleFreeHook;
    ProfDumpHook dump_hook = testDumpHook;
    ProfSampleHook old_sample_hook = testSampleHook;
    size_t size = sizeof(old_sample_hook);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_sample", &old_sample_hook, &size, &sample_hook, sizeof(sample_hook)), 0);
    CHECK(old_sample_hook == nullptr);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_sample_free", nullptr, nullptr, &free_hook, sizeof(free_hook)), 0);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_dump", nullptr, nullptr, &dump_hook, sizeof(dump_hook)), 0);

    setBacktrace({0x5555, 0x6666});
    void * p = sampledMalloc(2100000);
    CHECK(hook_ptr == p);
    CHECK_EQ(hook_size, size_t(2100000));
    CHECK_EQ(hook_usize, isalloc(&tsd, p));
    CHECK_EQ(hook_bt_len, 2u);
    CHECK(hook_bt0 == reinterpret_cast<void *>(0x5555));

    /// Dump files: explicit name, then automatic names `<prefix>.<pid>.<seq>.m<mseq>.heap`.
    const char * filename = "prof_test_explicit.heap";
    CHECK_EQ(ctlByName(tsd, "prof.dump", nullptr, nullptr, &filename, sizeof(filename)), 0);
    CHECK_EQ(hook_dump_filename, std::string(filename));
    FILE * file = std::fopen(filename, "r");
    REQUIRE(file != nullptr);
    std::string content;
    char buf[4096];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), file)) > 0)
        content.append(buf, n);
    std::fclose(file);
    unlink(filename);
    CHECK(content.starts_with("heap_v2/1\n"));
    CHECK(content.find("\n@ 0x5555 0x6666\n") != std::string::npos);
    CHECK(content.find("\nMAPPED_LIBRARIES:\n") != std::string::npos);

    const char * prefix = "prof_test_auto";
    CHECK_EQ(ctlByName(tsd, "prof.prefix", nullptr, nullptr, &prefix, sizeof(prefix)), 0);
    for (int i = 0; i < 2; ++i)
    {
        CHECK_EQ(ctlByName(tsd, "prof.dump", nullptr, nullptr, nullptr, 0), 0);
        std::string expected_name = fmt("prof_test_auto.%d.%d.m%d.heap", int(getpid()), i, i);
        CHECK_EQ(hook_dump_filename, expected_name);
        CHECK_EQ(access(expected_name.c_str(), R_OK), 0);
        unlink(expected_name.c_str());
    }
    /// `opt.prof_prefix` keeps the boot value.
    const char * opt_prefix = nullptr;
    size = sizeof(opt_prefix);
    CHECK_EQ(ctlByName(tsd, "opt.prof_prefix", &opt_prefix, &size, nullptr, 0), 0);
    CHECK_STREQ(opt_prefix, "jeprof");

    size_t usize = isalloc(&tsd, p);
    sampledFree(p);
    CHECK(hook_free_ptr == p);
    CHECK_EQ(hook_free_usize, usize);

    ProfSampleHook null_sample = nullptr;
    ProfSampleFreeHook null_free = nullptr;
    ProfDumpHook null_dump = nullptr;
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_sample", nullptr, nullptr, &null_sample, sizeof(null_sample)), 0);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_sample_free", nullptr, nullptr, &null_free, sizeof(null_free)), 0);
    CHECK_EQ(ctlByName(tsd, "experimental.hooks.prof_dump", nullptr, nullptr, &null_dump, sizeof(null_dump)), 0);
}

TEST(Prof, RecentAllocations)
{
    init();
    installBacktraceHook();
    ThreadState & tsd = ThreadState::fetch();
    uint64_t uid = mainThrUid();

    ssize_t max = 2;
    ssize_t old_max = -5;
    size_t size = sizeof(old_max);
    CHECK_EQ(ctlByName(tsd, "experimental.prof_recent.alloc_max", &old_max, &size, &max, sizeof(max)), 0);
    CHECK_EQ(old_max, ssize_t(0));
    ssize_t bad = -2;
    CHECK_EQ(ctlByName(tsd, "experimental.prof_recent.alloc_max", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);

    setBacktrace({0x8888});
    void * p1 = sampledMalloc(2070000);
    void * p2 = sampledMalloc(2080000);
    void * p3 = sampledMalloc(2090000);
    sampledFree(p3);

    std::string json;
    struct
    {
        WriteCallback * write_cb;
        void * cbopaque;
    } packet = {appendCallback, &json};
    CHECK_EQ(ctlByName(tsd, "experimental.prof_recent.alloc_dump", nullptr, nullptr, &packet, sizeof(packet)), 0);

    std::string prefix = fmt(
        "{\"sample_interval\":1,\"recent_alloc_max\":2,\"recent_alloc\":[{\"size\":2080000,\"usize\":%zu,\"released\":false,"
        "\"alloc_thread_uid\":%llu,\"alloc_thread_name\":\"prof test\",\"alloc_time\":",
        isalloc(&tsd, p2),
        (unsigned long long)uid);
    CHECK(json.starts_with(prefix));
    CHECK(json.find("\"alloc_trace\":[\"0x8888\"]}") != std::string::npos);
    CHECK(json.find("{\"size\":2090000,") != std::string::npos);
    CHECK(json.find("\"released\":true,") != std::string::npos);
    CHECK(json.find("\"dalloc_thread_uid\":") != std::string::npos);
    CHECK(json.find("\"dalloc_trace\":[\"0x8888\"]}]}") != std::string::npos);
    CHECK(json.ends_with("]}"));

    max = 0;
    CHECK_EQ(ctlByName(tsd, "experimental.prof_recent.alloc_max", nullptr, nullptr, &max, sizeof(max)), 0);
    sampledFree(p1);
    sampledFree(p2);
    CHECK_EQ(profBtCount(), size_t(0));
}
