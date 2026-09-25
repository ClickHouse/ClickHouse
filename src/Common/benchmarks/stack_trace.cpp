/// Cost of capturing a `StackTrace`, which every `DB::Exception` and every query profiler sample pays.
///   - `BM_StackTraceCapture/<depth>`: capture below `depth` extra frames of one function.
///   - `BM_StackTraceRememberState/<pairs>`: capture from a function whose unwind info carries `pairs`
///     `DW_CFA_remember_state`/`DW_CFA_restore_state` pairs, as a function with many epilogues does.
///   - `BM_ExceptionThrowCatch/<depth>`: throw and catch a `DB::Exception` below `depth` extra frames.

#include <benchmark/benchmark.h>

#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <base/defines.h>
#include <base/phdr_cache.h>

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

/// The pairs sit before the call, so unwinding from its return address interprets all of them.
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

}
