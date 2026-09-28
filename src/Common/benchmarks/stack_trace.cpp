/// Cost of capturing a `StackTrace`, which every `DB::Exception` and every query profiler sample pays.
///   - `BM_StackTraceCapture/<depth>`: capture below `depth` extra frames of one function.
///   - `BM_StackTraceRememberState/<pairs>`: capture from a function whose unwind info carries `pairs`
///     `DW_CFA_remember_state`/`DW_CFA_restore_state` pairs, as a function with many epilogues does.
///   - `BM_ExceptionThrowCatch/<depth>`: throw and catch a `DB::Exception` below `depth` extra frames.
///   - `BM_StackTraceDistinctSites/<depth>`: capture through `depth` call sites that change every time.
///   - `BM_StackTraceProfilerSample/<depth>`: capture from a signal handler interrupting a thread that runs
///     through such call sites, as the query profiler does.
/// The first three repeat the same stack, so after the first iteration everything the unwinder derives
/// from it can be reused; the last two measure the cost when it cannot.

#include <benchmark/benchmark.h>

#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <base/defines.h>
#include <base/phdr_cache.h>

#include <atomic>
#include <csignal>
#include <pthread.h>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// The server fills this cache at startup; without it every frame looks up the real `dl_iterate_phdr`.
[[maybe_unused]] const bool phdr_cache_ready = (updatePHDRCache(), true);

/// Escaping the result after the call keeps each level a real frame: a plain `+ 1` is still folded
/// into a loop by accumulator tail recursion elimination.
size_t NO_INLINE captureAtDepth(size_t depth)
{
    if (depth == 0)
    {
        StackTrace trace;
        return trace.getSize();
    }
    size_t size = captureAtDepth(depth - 1);
    benchmark::DoNotOptimize(size);
    return size;
}

void BM_StackTraceCapture(benchmark::State & state)
{
    const auto depth = static_cast<size_t>(state.range(0));
    size_t frames = 0;
    for (auto _ [[maybe_unused]] : state)
    {
        frames = captureAtDepth(depth);
        benchmark::DoNotOptimize(frames);
    }
    state.counters["frames"] = benchmark::Counter(static_cast<double>(frames), benchmark::Counter::kAvgThreads);
}
BENCHMARK(BM_StackTraceCapture)->Arg(0)->Arg(16)->Arg(64);
BENCHMARK(BM_StackTraceCapture)->Arg(16)->Threads(8)->UseRealTime();

/// A function whose unwind info looks like that of a function with `pairs` epilogues in the middle of its
/// code: the compiler brackets each of those with `.cfi_remember_state` and `.cfi_restore_state`.
/// Those directives emit no instructions, only the `DW_CFA_remember_state` and `DW_CFA_restore_state`
/// opcodes in the function's FDE. `.rept %c0` ... `.endr` repeats them `pairs` times (`%c0` prints the
/// constant operand without the `$` prefix). They sit before the call, so unwinding from its return
/// address interprets all of them.
/// Adding `pairs` keeps the instantiations' code distinct: they would otherwise differ only in unwind
/// info, and identical code folding would merge them into one.
template <size_t pairs>
size_t NO_INLINE captureAfterRememberState()
{
    __asm__ __volatile__(".rept %c0\n.cfi_remember_state\n.cfi_restore_state\n.endr" : : "i"(pairs));
    StackTrace trace;
    return trace.getSize() + pairs;
}

template <size_t pairs>
void BM_StackTraceRememberState(benchmark::State & state)
{
    for (auto _ [[maybe_unused]] : state)
        benchmark::DoNotOptimize(captureAfterRememberState<pairs>());
}
BENCHMARK_TEMPLATE(BM_StackTraceRememberState, 0);
BENCHMARK_TEMPLATE(BM_StackTraceRememberState, 1);
BENCHMARK_TEMPLATE(BM_StackTraceRememberState, 16);
BENCHMARK_TEMPLATE(BM_StackTraceRememberState, 200);

void NO_INLINE throwAtDepth(size_t depth)
{
    if (depth == 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "benchmark");
    throwAtDepth(depth - 1);
    benchmark::ClobberMemory();
}

void BM_ExceptionThrowCatch(benchmark::State & state)
{
    const auto depth = static_cast<size_t>(state.range(0));
    for (auto _ [[maybe_unused]] : state)
    {
        try
        {
            throwAtDepth(depth);
        }
        catch (const DB::Exception & e)
        {
            benchmark::DoNotOptimize(e.code());
        }
    }
}
BENCHMARK(BM_ExceptionThrowCatch)->Arg(0)->Arg(16);
BENCHMARK(BM_ExceptionThrowCatch)->Arg(16)->Threads(8)->UseRealTime();

/// Call sites that no unwinder cache can keep: four times the 4096 rules libunwind caches, so a site
/// visited again has been evicted by the others in between and its frame is decoded every time.
constexpr size_t num_distinct_sites = 16384;

size_t NO_INLINE siteBody(size_t index, size_t depth, bool capture);

/// Case `n` calls through its own call instruction, so every site has its own return address, which is
/// what an unwinder cache is keyed on. The empty `asm` with the case's constant before and after the
/// call keeps the compiler from hoisting or merging the identical calls into one.
#define SITE(n) \
    case (n): \
        __asm__ __volatile__("" : : "i"(n) : "memory"); \
        result = siteBody(index, depth, capture); \
        __asm__ __volatile__("" : : "i"(n) : "memory"); \
        break;
#define SITES_4(n) SITE(n) SITE((n) + 1) SITE((n) + 2) SITE((n) + 3)
#define SITES_16(n) SITES_4(n) SITES_4((n) + 4) SITES_4((n) + 8) SITES_4((n) + 12)
#define SITES_64(n) SITES_16(n) SITES_16((n) + 16) SITES_16((n) + 32) SITES_16((n) + 48)
#define SITES_256(n) SITES_64(n) SITES_64((n) + 64) SITES_64((n) + 128) SITES_64((n) + 192)
#define SITES_1024(n) SITES_256(n) SITES_256((n) + 256) SITES_256((n) + 512) SITES_256((n) + 768)
#define SITES_4096(n) SITES_1024(n) SITES_1024((n) + 1024) SITES_1024((n) + 2048) SITES_1024((n) + 3072)

size_t NO_INLINE site(size_t index, size_t depth, bool capture)
{
    size_t result = 0;
    switch (index)
    {
        SITES_4096(0)
        SITES_4096(4096)
        SITES_4096(8192)
        SITES_4096(12288)
        default:
            break;
    }
    benchmark::DoNotOptimize(result);
    return result;
}

#undef SITES_4096
#undef SITES_1024
#undef SITES_256
#undef SITES_64
#undef SITES_16
#undef SITES_4
#undef SITE

size_t NO_INLINE captureFromSite()
{
    StackTrace trace;
    return trace.getSize();
}

/// Goes `depth` distinct sites deeper, then captures or, for the sampled thread, just works.
size_t NO_INLINE siteBody(size_t index, size_t depth, bool capture)
{
    if (depth > 0)
        return site((index + 1) % num_distinct_sites, depth - 1, capture);
    if (capture)
        return captureFromSite();
    uint64_t x = index;
    for (int i = 0; i < 256; ++i)
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    return x;
}

/// Every capture goes through `depth` sites no earlier capture used recently, so each of those frames
/// misses the cache; the frames above them (this function, the benchmark driver) are the same every time.
void BM_StackTraceDistinctSites(benchmark::State & state)
{
    const auto depth = static_cast<size_t>(state.range(0));
    size_t next = 0;
    for (auto _ [[maybe_unused]] : state)
    {
        benchmark::DoNotOptimize(site(next, depth, true));
        next = (next + depth + 1) % num_distinct_sites;
    }
}
BENCHMARK(BM_StackTraceDistinctSites)->Arg(0)->Arg(8)->Arg(32);

/// Samples like the query profiler: a signal interrupts a thread at whatever instruction it is running,
/// and the handler captures from the signal context. The thread runs through all the distinct sites, so
/// the interrupted instruction and most frames above it change from one sample to the next.
std::atomic<int> sample_state{0};
std::atomic<size_t> sampled_frames{0};

void sampleHandler(int, siginfo_t *, void * context)
{
    StackTrace trace(*static_cast<const ucontext_t *>(context));
    sampled_frames.store(trace.getSize(), std::memory_order_relaxed);
    sample_state.store(2, std::memory_order_release);
}

void BM_StackTraceProfilerSample(benchmark::State & state)
{
    const auto depth = static_cast<size_t>(state.range(0));
    struct sigaction action{};
    action.sa_sigaction = sampleHandler;
    action.sa_flags = SA_SIGINFO | SA_RESTART;
    sigaction(SIGUSR2, &action, nullptr);

    std::atomic<bool> stop{false};
    std::thread sampled([&]
    {
        size_t next = 0;
        while (!stop.load(std::memory_order_relaxed))
        {
            benchmark::DoNotOptimize(site(next, depth, false));
            next = (next + depth + 1) % num_distinct_sites;
        }
    });

    for (auto _ [[maybe_unused]] : state)
    {
        sample_state.store(1, std::memory_order_relaxed);
        pthread_kill(sampled.native_handle(), SIGUSR2);
        while (sample_state.load(std::memory_order_acquire) != 2)
            ;
    }
    stop = true;
    sampled.join();
    state.counters["frames"] = static_cast<double>(sampled_frames.load());
}
BENCHMARK(BM_StackTraceProfilerSample)->Arg(0)->Arg(8)->Arg(32)->UseRealTime();

}
