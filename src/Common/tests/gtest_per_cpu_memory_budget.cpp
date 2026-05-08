#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/PerCPUMemoryBudget.h>
#include <Common/ThreadStatus.h>

#include "config.h"

using namespace DB;

namespace
{

void resetTrackers()
{
    MainThreadStatus::getInstance();
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
}

}

/// Sanity: per-CPU CPU detection works on this platform, and on Linux+rseq
/// the rseq fast path is active.
TEST(PerCPUMemoryBudget, Basic)
{
    resetTrackers();

    int cpu = PerCPUMemoryBudget::currentCPU();
    if (cpu < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

#if USE_LIBRSEQ
    EXPECT_TRUE(PerCPUMemoryBudget::isRSeqReady());
#endif
}

/// `chargeAlloc` advances `state.nallocs` by `size`. Within a single SLICE
/// bucket the call must not request a flush; crossing the bucket boundary
/// must request one. The counter is shared with every other thread on this
/// CPU, so we phrase the test in terms of "how far to the next boundary"
/// rather than absolute values.
TEST(PerCPUMemoryBudget, AllocCrossingDetection)
{
    resetTrackers();

    PerCPUMemoryBudget::PerCPUMemoryBudgetState state;
    state.cpu = PerCPUMemoryBudget::currentCPU();
    if (state.cpu < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

    /// Warm up: a zero-sized charge updates state.nallocs to the current
    /// per-CPU counter value without moving it. After this we have a
    /// meaningful baseline for the boundary-distance arithmetic below.
    std::ignore = PerCPUMemoryBudget::chargeAlloc(0, state);

    constexpr Int64 SLICE = PerCPUMemoryBudget::SLICE;
    Int64 to_boundary = SLICE - (state.nallocs & (SLICE - 1));

    EXPECT_FALSE(PerCPUMemoryBudget::chargeAlloc(to_boundary - 1, state));
    EXPECT_TRUE (PerCPUMemoryBudget::chargeAlloc(2, state));
    EXPECT_FALSE(PerCPUMemoryBudget::chargeAlloc(1, state));
}

/// `chargeFree` is symmetric with `chargeAlloc` but operates on the
/// independent `nfrees` counter — they don't interfere.
TEST(PerCPUMemoryBudget, FreeCrossingDetection)
{
    resetTrackers();

    PerCPUMemoryBudget::PerCPUMemoryBudgetState state;
    state.cpu = PerCPUMemoryBudget::currentCPU();
    if (state.cpu < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

    std::ignore = PerCPUMemoryBudget::chargeFree(0, state);

    constexpr Int64 SLICE = PerCPUMemoryBudget::SLICE;
    Int64 to_boundary = SLICE - (state.nfrees & (SLICE - 1));

    EXPECT_FALSE(PerCPUMemoryBudget::chargeFree(to_boundary - 1, state));
    EXPECT_TRUE (PerCPUMemoryBudget::chargeFree(2, state));
    EXPECT_FALSE(PerCPUMemoryBudget::chargeFree(1, state));
}

/// A single alloc/free pair on the same thread must not change
/// `total_memory_tracker.amount` once both flushes have run.
TEST(PerCPUMemoryBudget, BalancedAllocFreeDoesNotInflateTracker)
{
    resetTrackers();

    constexpr Int64 size = 16 * 1024 * 1024;
    constexpr Int64 slack = 1 * 1024 * 1024;

    const Int64 before = total_memory_tracker.get();

    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
    CurrentThread::flushUntrackedMemory();
    std::ignore = CurrentMemoryTracker::free(size);
    CurrentThread::flushUntrackedMemory();

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}

/// Repeated balanced pairs must not drift either: the per-thread
/// accumulator flips signs across iterations and any sign bug compounds
/// into a large drift.
TEST(PerCPUMemoryBudget, RepeatedAllocFreePairsDoNotDrift)
{
    resetTrackers();

    constexpr Int64 size = 8 * 1024 * 1024;
    constexpr int iterations = 64;
    constexpr Int64 slack = 1 * 1024 * 1024;

    const Int64 before = total_memory_tracker.get();

    for (int i = 0; i < iterations; ++i)
    {
        std::ignore = CurrentMemoryTracker::allocNoThrow(size);
        std::ignore = CurrentMemoryTracker::free(size);
    }
    CurrentThread::flushUntrackedMemory();

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}

/// Flush triggered purely by the per-CPU branch (per-thread limit raised
/// far above what we will accumulate). Verifies the per-CPU path itself
/// preserves the tracker.
TEST(PerCPUMemoryBudget, PerCpuOnlyFlushPreservesTracker)
{
    resetTrackers();

    const Int64 saved_limit = CurrentThread::get().untracked_memory_limit;
    CurrentThread::get().untracked_memory_limit = static_cast<Int64>(1) << 30;

    constexpr Int64 size = 16 * 1024 * 1024;
    constexpr Int64 slack = 1 * 1024 * 1024;

    const Int64 before = total_memory_tracker.get();

    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
    std::ignore = CurrentMemoryTracker::free(size);
    CurrentThread::flushUntrackedMemory();

    CurrentThread::get().untracked_memory_limit = saved_limit;

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}

/// Concurrent balanced alloc/free across many threads must not drift the
/// global tracker. Each worker installs a ThreadStatus so it owns a
/// per-thread untracked accumulator and exercises the per-CPU charge path.
/// Sizes vary across powers of two from 1 KiB up to 8 MiB so each
/// iteration follows a different flush regime.
TEST(PerCPUMemoryBudget, ConcurrentAllocFreeAcrossThreadsDoesNotDrift)
{
    resetTrackers();

    constexpr int num_threads = 8;
    constexpr int iterations = 128;
    constexpr int min_size_log2 = 10;
    constexpr int max_size_log2 = 23;
    constexpr int size_classes = max_size_log2 - min_size_log2 + 1;

    const Int64 before = total_memory_tracker.get();

    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            ThreadStatus thread_status;
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            for (int i = 0; i < iterations; ++i)
            {
                const Int64 size = static_cast<Int64>(1) << (min_size_log2 + ((t + i) % size_classes));
                std::ignore = CurrentMemoryTracker::allocNoThrow(size);
                std::ignore = CurrentMemoryTracker::free(size);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (auto & t : threads)
        t.join();

    constexpr Int64 slack = static_cast<Int64>(num_threads) * 4 * PerCPUMemoryBudget::SLICE;
    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}
