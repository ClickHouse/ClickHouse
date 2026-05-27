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

#if OS_LINUX
    EXPECT_TRUE(PerCPUMemoryBudget::isRSeqReady());
#endif
}

/// Sub-buffer charges stay in the per-thread accumulator and never reach
/// the per-CPU counter. The first charge with a size >= BUFFER_SIZE flushes
/// the buffer to the per-CPU layer.
TEST(PerCPUMemoryBudget, BufferDelaysPerCPUTouch)
{
    resetTrackers();

    PerCPUMemoryBudget::PerCPUMemoryBudgetState state;
    if (PerCPUMemoryBudget::currentCPU() < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

    constexpr UInt64 BUFFER_SIZE = PerCPUMemoryBudget::BUFFER_SIZE;

    /// Each sub-buffer charge bumps state.pending_alloc but never touches
    /// `state.nallocs` (which would happen on a per-CPU flush).
    EXPECT_FALSE(PerCPUMemoryBudget::chargeAlloc(BUFFER_SIZE / 4, state));
    EXPECT_FALSE(PerCPUMemoryBudget::chargeAlloc(BUFFER_SIZE / 4, state));
    EXPECT_EQ(state.nallocs, 0u);
    EXPECT_EQ(state.pending_alloc, BUFFER_SIZE / 2);

    /// Now exceed the buffer — this charge flushes the accumulated bytes to
    /// the per-CPU counter; `state.nallocs` becomes non-zero.
    std::ignore = PerCPUMemoryBudget::chargeAlloc(BUFFER_SIZE, state);
    EXPECT_GT(state.nallocs, 0u);
    EXPECT_EQ(state.pending_alloc, 0u);

    /// Symmetric for the free side.
    EXPECT_FALSE(PerCPUMemoryBudget::chargeFree(BUFFER_SIZE / 2, state));
    EXPECT_EQ(state.nfrees, 0u);
    EXPECT_EQ(state.pending_free, BUFFER_SIZE / 2);
    std::ignore = PerCPUMemoryBudget::chargeFree(BUFFER_SIZE, state);
    EXPECT_GT(state.nfrees, 0u);
    EXPECT_EQ(state.pending_free, 0u);
}

/// Crossing detection still fires when a per-CPU charge actually crosses a
/// SLICE boundary. We use a single charge of size `SLICE` so the boundary
/// is unambiguously crossed in one step (regardless of where on the CPU's
/// counter we start).
TEST(PerCPUMemoryBudget, AllocCrossingDetection)
{
    resetTrackers();

    PerCPUMemoryBudget::PerCPUMemoryBudgetState state;
    if (PerCPUMemoryBudget::currentCPU() < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

    constexpr UInt64 SLICE = PerCPUMemoryBudget::SLICE;

    /// Warm-up: charge >= BUFFER_SIZE so the per-CPU counter is touched and
    /// state.nallocs reflects a real position.
    std::ignore = PerCPUMemoryBudget::chargeAlloc(SLICE, state);
    /// Next per-CPU touch must be a charge of at least one more SLICE to
    /// guarantee a bucket change.
    EXPECT_TRUE(PerCPUMemoryBudget::chargeAlloc(SLICE, state));
}

/// Symmetric crossing test for the free side.
TEST(PerCPUMemoryBudget, FreeCrossingDetection)
{
    resetTrackers();

    PerCPUMemoryBudget::PerCPUMemoryBudgetState state;
    if (PerCPUMemoryBudget::currentCPU() < 0)
        GTEST_SKIP() << "Per-CPU machinery unavailable on this platform";

    constexpr UInt64 SLICE = PerCPUMemoryBudget::SLICE;

    std::ignore = PerCPUMemoryBudget::chargeFree(SLICE, state);
    EXPECT_TRUE(PerCPUMemoryBudget::chargeFree(SLICE, state));
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

    constexpr Int64 slack = static_cast<Int64>(num_threads * 4 * PerCPUMemoryBudget::SLICE);
    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}
