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
}

}

TEST(PerCPUMemoryBudget, Basic)
{
    resetTrackers();

    std::ignore = PerCPUMemoryBudget::charge(0);

    EXPECT_TRUE(PerCPUMemoryBudget::isReady());
#ifdef USE_LIBRSEQ
    EXPECT_TRUE(PerCPUMemoryBudget::isRSeqReady());
#endif
    EXPECT_GT(PerCPUMemoryBudget::reservedBytes(), 0);
    EXPECT_GE(PerCPUMemoryBudget::reservedBytes(), PerCPUMemoryBudget::SLICE);
}

TEST(PerCPUMemoryBudget, ChargeAdjustsReservedBytes)
{
    resetTrackers();
    std::ignore = PerCPUMemoryBudget::charge(0);
    ASSERT_TRUE(PerCPUMemoryBudget::isReady());

    const Int64 before = PerCPUMemoryBudget::reservedBytes();

    constexpr Int64 delta = 12345;
    std::ignore = PerCPUMemoryBudget::charge(delta);
    EXPECT_EQ(PerCPUMemoryBudget::reservedBytes(), before - delta);

    std::ignore = PerCPUMemoryBudget::charge(-delta);
    EXPECT_EQ(PerCPUMemoryBudget::reservedBytes(), before);
}

TEST(PerCPUMemoryBudget, OutOfBoundsChargeRequestsFlush)
{
    resetTrackers();
    std::ignore = PerCPUMemoryBudget::charge(0);
    ASSERT_TRUE(PerCPUMemoryBudget::isReady());

    const Int64 huge = 3 * PerCPUMemoryBudget::SLICE;

    EXPECT_TRUE(PerCPUMemoryBudget::charge(huge));
    /// restore the slot so we do not poison neighbouring tests
    std::ignore = PerCPUMemoryBudget::charge(-huge);
}

TEST(PerCPUMemoryBudget, BalancedAllocFreeDoesNotInflateTracker)
{
    resetTrackers();

    /// 16MiB is guaranteed to be flushed to the MemoryTracker
    constexpr Int64 size = 16 * 1024 * 1024;
    const Int64 before = total_memory_tracker.get();

    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
    std::ignore = CurrentMemoryTracker::free(size);

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_EQ(drift, 0);
}

TEST(PerCPUMemoryBudget, RepeatedAllocFreePairsDoNotDrift)
{
    resetTrackers();

    constexpr Int64 size = 8 * 1024 * 1024; /// crosses the per-thread limit
    constexpr int iterations = 64;

    const Int64 before = total_memory_tracker.get();

    for (int i = 0; i < iterations; ++i)
    {
        std::ignore = CurrentMemoryTracker::allocNoThrow(size);
        std::ignore = CurrentMemoryTracker::free(size);
    }

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_EQ(drift, 0);
}

/// A free flush triggered purely by the per-CPU branch (not by the
/// per-thread limit) must still bring total_memory_tracker.amount back to
/// where it was before the matching allocation. This isolates the sign
/// handling in the per-CPU branch from the per-thread limit branch.
TEST(PerCPUMemoryBudget, PerCpuOnlyFlushPreservesTracker)
{
    resetTrackers();

    /// Raise the per-thread limit so it never trips; the only flush path
    /// available is the per-CPU one.
    const Int64 saved_limit = CurrentThread::get().untracked_memory_limit;
    CurrentThread::get().untracked_memory_limit = 1 << 30;

    /// 16 MiB > 2 * SLICE forces the per-CPU branch on the first charge.
    constexpr Int64 size = 16 * 1024 * 1024;

    const Int64 before = total_memory_tracker.get();

    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
    std::ignore = CurrentMemoryTracker::free(size);

    CurrentThread::get().untracked_memory_limit = saved_limit;

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_EQ(drift, 0);
}

TEST(PerCPUMemoryBudget, ConcurrentAllocFreeAcrossThreadsDoesNotDrift)
{
    resetTrackers();

    constexpr Int64 num_threads = 8;
    constexpr Int64 iterations = 128;
    constexpr int min_size_log2 = 10; /// 1 KiB
    constexpr int max_size_log2 = 23; /// 8 MiB
    constexpr int size_classes = max_size_log2 - min_size_log2 + 1;

    current_thread->flushUntrackedMemory();
    const Int64 before = total_memory_tracker.get();

    {
        std::atomic<bool> start{false};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (Int64 t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&, t]()
            {
                ThreadStatus thread_status;
                while (!start.load(std::memory_order_acquire))
                    std::this_thread::yield();

                for (Int64 i = 0; i < iterations; ++i)
                {
                    const Int64 size = 1LL << (min_size_log2 + ((t + i) % size_classes));
                    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
                    std::ignore = CurrentMemoryTracker::free(size);
                }
                /// ~ThreadStatus flushes the per-thread accumulator via the
                /// (correct) adjustWithUntrackedMemory path on the way out.
            });
        }
        start.store(true, std::memory_order_release);
        for (auto & t : threads)
            t.join();
    }
    current_thread->flushUntrackedMemory();

    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_EQ(drift, 0);
}
