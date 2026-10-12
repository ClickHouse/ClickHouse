/// Heap profiling: the sampling wait values (pinned from jemalloc's C formula), the unbiasing tables, the tdata /
/// tctx / gctx life cycles, `prof.reset`, the recent allocation records, the hooks and the exact heap dump format.
///
/// The allocator is initialized with `log2_profiling_sample:0` and a deterministic backtrace hook; sampled allocations are
/// made through the same inline functions the front-end uses (`profilingAllocPrepare`, `profilingMalloc`, `profilingFree`).

#include <allocator/Frontend.h>
#include <allocator/Init.h>
#include <allocator/Mallctl.h>
#include <allocator/Options.h>
#include <allocator/Profiling.h>
#include <allocator/ProfilingHooks.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <pthread.h>
#include <unistd.h>

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
    REQUIRE(options.profiling);
    REQUIRE(log2_profiling_sample == 0);
}

/// The backtrace returned by the test hook.
void * test_backtrace[8];
unsigned test_backtrace_len = 0;

void testBacktraceHook(void ** vector, unsigned * len, unsigned max_len)
{
    REQUIRE(test_backtrace_len <= max_len);
    for (unsigned i = 0; i < test_backtrace_len; ++i)
        vector[i] = test_backtrace[i];
    *len = test_backtrace_len;
}

void setBacktrace(std::initializer_list<uintptr_t> frames)
{
    test_backtrace_len = 0;
    for (uintptr_t frame : frames)
        test_backtrace[test_backtrace_len++] = reinterpret_cast<void *>(frame);
}

void installBacktraceHook()
{
    ThreadState & thread_state = ThreadState::fetch();
    ProfilingBacktraceHook hook = testBacktraceHook;
    ProfilingBacktraceHook old_hook = nullptr;
    size_t old_size = sizeof(old_hook);
    REQUIRE(mallctlByName(thread_state, "experimental.hooks.prof_backtrace", &old_hook, &old_size, &hook, sizeof(hook)) == 0);
    REQUIRE(old_hook == testBacktraceHook || old_hook == profilingBacktraceImpl);
}

/// A sampled large allocation made like `imalloc_body` does (always sampled: `sample_event = true`). Small sizes
/// would need the promotion of `imalloc_sample`, which is not done here.
void * sampledMalloc(size_t size)
{
    ThreadState & thread_state = ThreadState::fetch();
    /// `imalloc_sample` of a large size: page-aligned, so that the free path can recognize sampled objects.
    size_t usable_size = size_classes::alignedSizeToUsableSize(size, PROFILING_SAMPLE_ALIGNMENT);
    ProfilingThreadContext * thread_context = profilingAllocPrepare(thread_state, profilingActiveGetUnlocked(), true);
    REQUIRE(profilingThreadContextIsValid(thread_context));
    void * ptr = internalAllocateAligned(thread_state, usable_size, PROFILING_SAMPLE_ALIGNMENT, false);
    REQUIRE(ptr != nullptr);
    profilingMalloc(thread_state, ptr, size, usable_size, nullptr, thread_context);
    return ptr;
}

void sampledFree(void * ptr)
{
    ThreadState & thread_state = ThreadState::fetch();
    size_t usable_size = allocationSize(&thread_state, ptr);
    profilingFree(thread_state, ptr, usable_size, nullptr);
    internalDeallocate(thread_state, ptr);
}

void appendCallback(void * opaque, const char * s)
{
    static_cast<std::string *>(opaque)->append(s);
}

/// `prof_dump` without the file: the output of `profilingDumpImpl`.
std::string dumpToString()
{
    ThreadState & thread_state = ThreadState::fetch();
    std::string out;
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    REQUIRE(thread_data != nullptr);
    preReentrancy(thread_state, nullptr);
    profiling_dump_mutex.lock(&thread_state);
    profilingDumpImpl(thread_state, appendCallback, &out, thread_data, false);
    profiling_dump_mutex.unlock(&thread_state);
    postReentrancy(thread_state);
    return out;
}

/// Replaces the age of `f:` records with N and splits off the `frag_util:` lines (checking their format).
std::string normalizeDump(const std::string & dump, size_t * fragmentation_utilization_lines)
{
    std::string result;
    *fragmentation_utilization_lines = 0;
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
            ++*fragmentation_utilization_lines;
            continue;
        }
        CHECK_EQ(*fragmentation_utilization_lines, size_t(0)); /// `frag_util:` lines are last.
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

std::string formatToString(const char * format, ...) __attribute__((format(printf, 1, 2)));
std::string formatToString(const char * format, ...)
{
    char buf[1024];
    va_list args;
    va_start(args, format);
    std::vsnprintf(buf, sizeof(buf), format, args);
    va_end(args);
    return buf;
}

/// The unbiased size of one sample: `profiling_unbiased_size` uses the size of the class (not the usize, which differs with
/// `disable_large_size_classes`); with a rate of 1 byte the unbiasing is the identity otherwise.
size_t unbiasedSize(void * ptr)
{
    return size_classes::indexToSizeUnsafe(size_classes::sizeToIndex(allocationSize(&ThreadState::fetch(), ptr)));
}

uint64_t mainThreadUID()
{
    ThreadState & thread_state = ThreadState::fetch();
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    REQUIRE(thread_data != nullptr);
    return thread_data->thread_uid;
}

std::string fRecord(void * ptr, size_t size, uint64_t thread_uid)
{
    ThreadState & thread_state = ThreadState::fetch();
    size_t usable_size = allocationSize(&thread_state, ptr);
    Extent * extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
    return formatToString(
        "  f: N %zu %zu %u %u %llu\n",
        size,
        usable_size,
        unsigned(size_classes::sizeToIndex(usable_size)),
        extent->arenaIdx(),
        (unsigned long long)thread_uid);
}

}

/// The waits of `prof_sample_new_event_wait` for fixed PRNG states, computed by the C formula.
TEST(Profiling, SampleNewEventWait)
{
    init();
    size_t saved_log2 = log2_profiling_sample;
    static ThreadState thread_state;

    log2_profiling_sample = 19;
    thread_state.prng_state = 42;
    const uint64_t expected_19[] = {296343, 780978, 463837, 241909, 202085};
    for (uint64_t expected : expected_19)
        CHECK_EQ(profilingSampleNewEventWait(thread_state), expected);

    log2_profiling_sample = 10;
    thread_state.prng_state = 0xfffff7ff0000ULL;
    const uint64_t expected_10[] = {1620, 2153, 463, 226, 1458};
    for (uint64_t expected : expected_10)
        CHECK_EQ(profilingSamplePostponedEventWait(thread_state), expected);

    /// lg_prof_sample == 0: sample every allocation; no PRNG draw.
    log2_profiling_sample = 0;
    thread_state.prng_state = 7;
    CHECK_EQ(profilingSampleNewEventWait(thread_state), uint64_t(1));
    CHECK_EQ(thread_state.prng_state, uint64_t(7));

    log2_profiling_sample = saved_log2;
}

TEST(Profiling, UnbiasMap)
{
    init();
    size_t saved_log2 = log2_profiling_sample;
    log2_profiling_sample = 19;
    profilingUnbiasMapInit();
    SizeClassIdx idx = size_classes::sizeToIndex(4096);
    CHECK_EQ(profiling_unbiased_size[idx], size_t(526339));
    CHECK_EQ(profiling_shifted_unbiased_count[idx], size_t(1028));
    log2_profiling_sample = saved_log2;
    profilingUnbiasMapInit();
    /// With a rate of 1 byte the unbiasing is the identity (for sizes >= 8).
    CHECK_EQ(profiling_unbiased_size[idx], size_t(4096));
    CHECK_EQ(profiling_shifted_unbiased_count[idx], size_t(8));
}

TEST(Profiling, MallctlErrors)
{
    init();
    ThreadState & thread_state = ThreadState::fetch();
    size_t value = 0;
    size_t size = sizeof(value);
    const char * filename = "x";
    CHECK_EQ(mallctlByName(thread_state, "prof.dump", &value, &size, &filename, sizeof(filename)), EPERM);
    CHECK_EQ(mallctlByName(thread_state, "prof.reset", nullptr, nullptr, &value, 1), EINVAL);
    CHECK_EQ(mallctlByName(thread_state, "prof.log_start", nullptr, nullptr, nullptr, 0), ENOENT);
    CHECK_EQ(mallctlByName(thread_state, "prof.log_stop", nullptr, nullptr, nullptr, 0), ENOENT);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_sample", nullptr, nullptr, nullptr, 0), EINVAL);
    ProfilingBacktraceHook null_hook = nullptr;
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_backtrace", nullptr, nullptr, &null_hook, sizeof(null_hook)), EINVAL);
    CHECK_EQ(mallctlByName(thread_state, "prof.stats.bins.0.live", nullptr, nullptr, nullptr, 0), ENOENT);

    uint64_t interval = 1;
    size = sizeof(interval);
    CHECK_EQ(mallctlByName(thread_state, "prof.interval", &interval, &size, nullptr, 0), 0);
    CHECK_EQ(interval, uint64_t(0));
    size = sizeof(value);
    CHECK_EQ(mallctlByName(thread_state, "prof.lg_sample", &value, &size, nullptr, 0), 0);
    CHECK_EQ(value, size_t(0));

    bool active = false;
    size_t bool_size = sizeof(active);
    CHECK_EQ(mallctlByName(thread_state, "prof.active", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.active", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(mallctlByName(thread_state, "prof.thread_active_init", &active, &bool_size, nullptr, 0), 0);
    CHECK(active);
    CHECK_EQ(mallctlByName(thread_state, "prof.gdump", &active, &bool_size, nullptr, 0), 0);
    CHECK(!active);
}

TEST(Profiling, ThreadName)
{
    init();
    ThreadState & thread_state = ThreadState::fetch();
    const char * bad = "a\x01";
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.name", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);
    const char * long_name = "0123456789abcdefghij";
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.name", nullptr, nullptr, &long_name, sizeof(long_name)), 0);
    const char * name = nullptr;
    size_t size = sizeof(name);
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.name", &name, &size, nullptr, 0), 0);
    CHECK_STREQ(name, "0123456789abcde");
    /// Reading and writing at the same time.
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.name", &name, &size, &long_name, sizeof(long_name)), EPERM);
    const char * good = "prof test";
    CHECK_EQ(mallctlByName(thread_state, "thread.prof.name", nullptr, nullptr, &good, sizeof(good)), 0);
}

/// The life cycle of the structures and the exact dump format; the first gctx of the process gets the lock 1023.
TEST(Profiling, DumpFormatAndLifecycle)
{
    init();
    installBacktraceHook();
    ThreadState & thread_state = ThreadState::fetch();
    uint64_t uid = mainThreadUID();
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, false);
    CHECK(thread_data->lock == &thread_data_locks[uid % PROFILING_NUM_THREAD_DATA_LOCKS]);
    CHECK_EQ(profilingBacktraceCount(), size_t(0));
    size_t all_thread_data_before = profilingThreadDataCount();

    setBacktrace({0x201});
    size_t size_a = (4 << 20) + 5;
    void * a = sampledMalloc(size_a);
    setBacktrace({0x102, 0x3000});
    size_t size_b = (3 << 20) + 17;
    void * b = sampledMalloc(size_b);
    /// A second allocation with the same backtrace (same tctx).
    void * b2 = sampledMalloc(size_b);

    CHECK_EQ(profilingBacktraceCount(), size_t(2));
    CHECK_EQ(thread_data->backtrace_to_thread_context.count(), size_t(2));
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before);

    Extent * extent_a = arena_extent_map_global.extentLookup(&thread_state, a);
    ProfilingThreadContext * thread_context_a = extent_a->profilingThreadContext();
    REQUIRE(profilingThreadContextIsValid(thread_context_a));
    CHECK(thread_context_a->global_context->lock == &global_context_locks[PROFILING_NUM_CONTEXT_LOCKS - 1]);
    ProfilingThreadContext * thread_context_b = arena_extent_map_global.extentLookup(&thread_state, b)->profilingThreadContext();
    CHECK(thread_context_b->global_context->lock == &global_context_locks[0]);
    CHECK_EQ(thread_context_a->thread_context_uid, uint64_t(0));
    CHECK_EQ(thread_context_b->thread_context_uid, uint64_t(1));
    CHECK_EQ(thread_context_b->counts.current_objects, uint64_t(2));

    size_t usable_size_a = allocationSize(&thread_state, a);
    size_t usable_size_b = allocationSize(&thread_state, b);
    size_t fragmentation_utilization_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    CHECK_GT(fragmentation_utilization_lines, size_t(0));

    auto expected_dump = [&](size_t bytes_a, size_t bytes_b)
    {
        std::string block_a
            = formatToString("@ 0x201\n  t*: 1: %zu [0: 0]\n  t%llu: 1: %zu [0: 0]\n", bytes_a, (unsigned long long)uid, bytes_a)
            + fRecord(a, size_a, uid);
        std::string block_b
            = formatToString(
                  "@ 0x102 0x3000\n  t*: 2: %zu [0: 0]\n  t%llu: 2: %zu [0: 0]\n", 2 * bytes_b, (unsigned long long)uid, 2 * bytes_b)
            + fRecord(b, size_b, uid) + fRecord(b2, size_b, uid);
        /// The blocks are ordered by `memcmp` of the raw program counters: 0x201 < 0x102 in little-endian byte order.
        std::string blocks = config::big_endian ? block_b + block_a : block_a + block_b;
        return formatToString(
                   "heap_v2/1\n  t*: 3: %zu [0: 0]\n  t%llu: 3: %zu [0: 0] prof test\n",
                   bytes_a + 2 * bytes_b,
                   (unsigned long long)uid,
                   bytes_a + 2 * bytes_b)
            + blocks;
    };
    std::string expected = expected_dump(unbiasedSize(a), unbiasedSize(b));
    CHECK_STREQ(dump.c_str(), expected.c_str());

    /// `profiling_unbias:false` prints the raw counters (the usizes).
    options.profiling_unbias = false;
    std::string raw = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    options.profiling_unbias = true;
    expected = expected_dump(usable_size_a, usable_size_b);
    CHECK_STREQ(raw.c_str(), expected.c_str());

    /// Freeing destroys the tctx and the gctx (and nothing is left in the dump).
    sampledFree(a);
    CHECK_EQ(profilingBacktraceCount(), size_t(1));
    CHECK_EQ(thread_data->backtrace_to_thread_context.count(), size_t(1));
    sampledFree(b);
    CHECK_EQ(profilingBacktraceCount(), size_t(1));
    sampledFree(b2);
    CHECK_EQ(profilingBacktraceCount(), size_t(0));
    CHECK_EQ(thread_data->backtrace_to_thread_context.count(), size_t(0));

    dump = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    expected = formatToString("heap_v2/1\n  t*: 0: 0 [0: 0]\n  t%llu: 0: 0 [0: 0] prof test\n", (unsigned long long)uid);
    CHECK_STREQ(dump.c_str(), expected.c_str());
}

namespace
{

struct ThreadResult
{
    uint64_t thread_uid = 0;
    void * ptr = nullptr;
    bool keep = false;
};

void * threadBody(void * arg)
{
    auto * result = static_cast<ThreadResult *>(arg);
    ThreadState & thread_state = ThreadState::fetch();
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    REQUIRE(thread_data != nullptr);
    result->thread_uid = thread_data->thread_uid;
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
TEST(Profiling, ThreadExit)
{
    init();
    installBacktraceHook();
    size_t all_thread_data_before = profilingThreadDataCount();

    ThreadResult first;
    pthread_t thread;
    REQUIRE(pthread_create(&thread, nullptr, threadBody, &first) == 0);
    pthread_join(thread, nullptr);
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before);

    ThreadResult second;
    second.keep = true;
    REQUIRE(pthread_create(&thread, nullptr, threadBody, &second) == 0);
    pthread_join(thread, nullptr);
    CHECK_EQ(second.thread_uid, first.thread_uid + 1);
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before + 1);

    size_t fragmentation_utilization_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    CHECK(
        dump.find(formatToString("\n  t%llu: 1: %zu [0: 0]\n", (unsigned long long)second.thread_uid, unbiasedSize(second.ptr)))
        != std::string::npos);
    CHECK(dump.find("@ 0x7777\n") != std::string::npos);

    sampledFree(second.ptr);
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before);
    CHECK_EQ(profilingBacktraceCount(), size_t(0));
}

/// `prof.reset` expires all tdatas: their samples vanish from the dumps (but their `f:` records stay listed under
/// their gctx); the thread gets a new tdata with the same uid and the next discriminator.
TEST(Profiling, Reset)
{
    init();
    installBacktraceHook();
    ThreadState & thread_state = ThreadState::fetch();
    uint64_t uid = mainThreadUID();
    ProfilingThreadData * old_thread_data = profilingThreadDataGet(thread_state, false);
    uint64_t old_discriminator = old_thread_data->thread_discriminator;
    size_t all_thread_data_before = profilingThreadDataCount();

    setBacktrace({0x4444});
    void * x = sampledMalloc(1 << 20);

    size_t log2_size = 100;
    CHECK_EQ(mallctlByName(thread_state, "prof.reset", nullptr, nullptr, &log2_size, sizeof(log2_size)), 0);
    CHECK_EQ(log2_profiling_sample, size_t(63));
    log2_size = 0;
    CHECK_EQ(mallctlByName(thread_state, "prof.reset", nullptr, nullptr, &log2_size, sizeof(log2_size)), 0);
    CHECK_EQ(log2_profiling_sample, size_t(0));
    CHECK(old_thread_data->expired);

    ProfilingThreadData * new_thread_data = profilingThreadDataGet(thread_state, true);
    CHECK(new_thread_data != old_thread_data);
    CHECK_EQ(new_thread_data->thread_uid, uid);
    CHECK_EQ(new_thread_data->thread_discriminator, old_discriminator + 1);
    CHECK_STREQ(new_thread_data->thread_name, "prof test");
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before + 1);

    /// The sample of the expired tdata is not counted; its gctx has no counters and is skipped.
    size_t fragmentation_utilization_lines = 0;
    std::string dump = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    std::string expected = formatToString("heap_v2/1\n  t*: 0: 0 [0: 0]\n  t%llu: 0: 0 [0: 0] prof test\n", (unsigned long long)uid);
    CHECK_STREQ(dump.c_str(), expected.c_str());

    /// A new sample with the same backtrace: the gctx is dumped again, with the `f:` record of the old sample too.
    void * y = sampledMalloc(1 << 20);
    dump = normalizeDump(dumpToString(), &fragmentation_utilization_lines);
    size_t bytes = unbiasedSize(y);
    expected = formatToString(
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
    CHECK_EQ(profilingThreadDataCount(), all_thread_data_before);
    sampledFree(y);
    CHECK_EQ(profilingBacktraceCount(), size_t(0));
}

namespace
{

const void * hook_ptr = nullptr;
size_t hook_size = 0;
size_t hook_usable_size = 0;
unsigned hook_backtrace_len = 0;
void * hook_backtrace0 = nullptr;
const void * hook_free_ptr = nullptr;
size_t hook_free_usable_size = 0;
std::string hook_dump_filename;

void testSampleHook(const void * ptr, size_t size, void ** backtrace, unsigned backtrace_length, size_t usable_size)
{
    hook_ptr = ptr;
    hook_size = size;
    hook_usable_size = usable_size;
    hook_backtrace_len = backtrace_length;
    hook_backtrace0 = backtrace[0];
    /// Inside the hook, the thread is reentrant.
    CHECK_GT(ThreadState::fetch().reentrancyLevel(), int8_t(0));
}

void testSampleFreeHook(const void * ptr, size_t usable_size)
{
    hook_free_ptr = ptr;
    hook_free_usable_size = usable_size;
}

void testDumpHook(const char * filename)
{
    hook_dump_filename = filename;
}

}

TEST(Profiling, Hooks)
{
    init();
    installBacktraceHook();
    ThreadState & thread_state = ThreadState::fetch();

    ProfilingSampleHook sample_hook = testSampleHook;
    ProfilingSampleFreeHook free_hook = testSampleFreeHook;
    ProfilingDumpHook dump_hook = testDumpHook;
    ProfilingSampleHook old_sample_hook = testSampleHook;
    size_t size = sizeof(old_sample_hook);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_sample", &old_sample_hook, &size, &sample_hook, sizeof(sample_hook)), 0);
    CHECK(old_sample_hook == nullptr);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_sample_free", nullptr, nullptr, &free_hook, sizeof(free_hook)), 0);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_dump", nullptr, nullptr, &dump_hook, sizeof(dump_hook)), 0);

    setBacktrace({0x5555, 0x6666});
    void * p = sampledMalloc(2100000);
    CHECK(hook_ptr == p);
    CHECK_EQ(hook_size, size_t(2100000));
    CHECK_EQ(hook_usable_size, allocationSize(&thread_state, p));
    CHECK_EQ(hook_backtrace_len, 2u);
    CHECK(hook_backtrace0 == reinterpret_cast<void *>(0x5555));

    /// Dump files: explicit name, then automatic names `<prefix>.<pid>.<seq>.m<mseq>.heap`.
    const char * filename = "prof_test_explicit.heap";
    CHECK_EQ(mallctlByName(thread_state, "prof.dump", nullptr, nullptr, &filename, sizeof(filename)), 0);
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
    CHECK_EQ(mallctlByName(thread_state, "prof.prefix", nullptr, nullptr, &prefix, sizeof(prefix)), 0);
    for (int i = 0; i < 2; ++i)
    {
        CHECK_EQ(mallctlByName(thread_state, "prof.dump", nullptr, nullptr, nullptr, 0), 0);
        std::string expected_name = formatToString("prof_test_auto.%d.%d.m%d.heap", int(getpid()), i, i);
        CHECK_EQ(hook_dump_filename, expected_name);
        CHECK_EQ(access(expected_name.c_str(), R_OK), 0);
        unlink(expected_name.c_str());
    }
    /// `opt.prof_prefix` keeps the boot value.
    const char * option_prefix = nullptr;
    size = sizeof(option_prefix);
    CHECK_EQ(mallctlByName(thread_state, "opt.prof_prefix", &option_prefix, &size, nullptr, 0), 0);
    CHECK_STREQ(option_prefix, "jeprof");

    size_t usable_size = allocationSize(&thread_state, p);
    sampledFree(p);
    CHECK(hook_free_ptr == p);
    CHECK_EQ(hook_free_usable_size, usable_size);

    ProfilingSampleHook null_sample = nullptr;
    ProfilingSampleFreeHook null_free = nullptr;
    ProfilingDumpHook null_dump = nullptr;
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_sample", nullptr, nullptr, &null_sample, sizeof(null_sample)), 0);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_sample_free", nullptr, nullptr, &null_free, sizeof(null_free)), 0);
    CHECK_EQ(mallctlByName(thread_state, "experimental.hooks.prof_dump", nullptr, nullptr, &null_dump, sizeof(null_dump)), 0);
}

TEST(Profiling, RecentAllocations)
{
    init();
    installBacktraceHook();
    ThreadState & thread_state = ThreadState::fetch();
    uint64_t uid = mainThreadUID();

    ssize_t max = 2;
    ssize_t old_max = -5;
    size_t size = sizeof(old_max);
    CHECK_EQ(mallctlByName(thread_state, "experimental.prof_recent.alloc_max", &old_max, &size, &max, sizeof(max)), 0);
    CHECK_EQ(old_max, ssize_t(0));
    ssize_t bad = -2;
    CHECK_EQ(mallctlByName(thread_state, "experimental.prof_recent.alloc_max", nullptr, nullptr, &bad, sizeof(bad)), EINVAL);

    setBacktrace({0x8888});
    void * p1 = sampledMalloc(2070000);
    void * p2 = sampledMalloc(2080000);
    void * p3 = sampledMalloc(2090000);
    sampledFree(p3);

    std::string json;
    struct
    {
        WriteCallback * write_callback;
        void * callback_argument;
    } packet = {appendCallback, &json};
    CHECK_EQ(mallctlByName(thread_state, "experimental.prof_recent.alloc_dump", nullptr, nullptr, &packet, sizeof(packet)), 0);

    std::string prefix = formatToString(
        "{\"sample_interval\":1,\"recent_alloc_max\":2,\"recent_alloc\":[{\"size\":2080000,\"usize\":%zu,\"released\":false,"
        "\"alloc_thread_uid\":%llu,\"alloc_thread_name\":\"prof test\",\"alloc_time\":",
        allocationSize(&thread_state, p2),
        (unsigned long long)uid);
    CHECK(json.starts_with(prefix));
    CHECK(json.find("\"alloc_trace\":[\"0x8888\"]}") != std::string::npos);
    CHECK(json.find("{\"size\":2090000,") != std::string::npos);
    CHECK(json.find("\"released\":true,") != std::string::npos);
    CHECK(json.find("\"dalloc_thread_uid\":") != std::string::npos);
    CHECK(json.find("\"dalloc_trace\":[\"0x8888\"]}]}") != std::string::npos);
    CHECK(json.ends_with("]}"));

    max = 0;
    CHECK_EQ(mallctlByName(thread_state, "experimental.prof_recent.alloc_max", nullptr, nullptr, &max, sizeof(max)), 0);
    sampledFree(p1);
    sampledFree(p2);
    CHECK_EQ(profilingBacktraceCount(), size_t(0));
}
