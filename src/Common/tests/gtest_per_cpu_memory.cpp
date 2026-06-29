#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/PerCPUMemory.h>
#include <Common/ThreadStatus.h>

#if defined(OS_LINUX)
#include <sched.h>
#endif

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

void resetTrackers()
{
    MainThreadStatus::getInstance();
    /// Release this thread's contribution from the global per_cpu_memory and restore defaults.
    CurrentThread::flushUntrackedMemory();
    per_cpu_memory.setThreadBuffer(PerCPUMemory::DEFAULT_THREAD_BUFFER);
    per_cpu_memory.setBudgetCapacity(PerCPUMemory::DEFAULT_BUDGET);
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
}

#if defined(OS_LINUX)
/// Pin the current thread to one CPU, so sync()'s sched_getcpu() is stable across a test.
bool pinTo(int cpu)
{
    cpu_set_t one;
    CPU_ZERO(&one);
    CPU_SET(cpu, &one);
    return sched_setaffinity(0, sizeof(one), &one) == 0 && sched_getcpu() == cpu;
}

/// CPUs this process may run on.
std::vector<int> allowedCpus()
{
    cpu_set_t set;
    std::vector<int> cpus;
    if (sched_getaffinity(0, sizeof(set), &set) != 0)
        return cpus;
    for (int c = 0; c < CPU_SETSIZE; ++c)
        if (CPU_ISSET(c, &set))
            cpus.push_back(c);
    return cpus;
}

/// Run `body` pinned to `cpu`, restoring the prior affinity afterwards. Returns false without
/// running `body` if the CPU could not be pinned, so the caller can GTEST_SKIP.
template <typename Body>
bool runOnCpu(int cpu, Body && body)
{
    cpu_set_t saved;
    if (sched_getaffinity(0, sizeof(saved), &saved) != 0 || !pinTo(cpu))
        return false;
    body();
    sched_setaffinity(0, sizeof(saved), &saved);
    return true;
}

/// runOnCpu on the first CPU that can be pinned; `body` receives the chosen CPU.
template <typename Body>
bool runOnAnyCpu(Body && body)
{
    for (int cpu : allowedCpus())
        if (runOnCpu(cpu, [&] { body(cpu); }))
            return true;
    return false;
}
#endif

}

#if defined(OS_LINUX)
/// A thread folds into the per-CPU counter only after drifting by `buffer`; the fold moves the
/// counter by the exact signed delta (both directions), and release backs it out.
TEST(PerCPUMemory, FoldsBufferedSignedDelta)
{
    const bool ran = runOnAnyCpu([&](int cpu)
    {
        const Int64 buffer = 32 * 1024;
        PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), 8 * 1024 * 1024, buffer);
        PerCPUMemoryThreadState state;

        /// Sub-buffer drift: no fold.
        EXPECT_TRUE(memory.sync(buffer / 2, state));
        EXPECT_EQ(state.contributed, 0);
        EXPECT_EQ(memory.netOnCPU(cpu), 0);

        /// Drift reaches the buffer: fold the exact value.
        EXPECT_TRUE(memory.sync(4 * buffer, state));
        EXPECT_EQ(state.contributed, 4 * buffer);
        EXPECT_EQ(memory.netOnCPU(cpu), 4 * buffer);

        /// Freeing back down folds the negative delta.
        EXPECT_TRUE(memory.sync(0, state));
        EXPECT_EQ(state.contributed, 0);
        EXPECT_EQ(memory.netOnCPU(cpu), 0);
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// allocated and freed are budgeted separately, so a deferred free cannot free up budget for a
/// deferred allocation.
TEST(PerCPUMemory, DeferredFreesDoNotAdmitAllocations)
{
    const bool ran = runOnAnyCpu([&](int cpu)
    {
        PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), /*capacity*/ 40 * 1024, /*buffer*/ 32 * 1024);
        PerCPUMemoryThreadState freeing;
        PerCPUMemoryThreadState allocating1;
        PerCPUMemoryThreadState allocating2;

        /// A thread defers -33 KiB of frees (fits the free side's budget). 33 KiB > the 32 KiB buffer,
        /// so each deferral below actually reaches the shared counter rather than the per-thread slack.
        EXPECT_TRUE(memory.sync(-33 * 1024, freeing));
        /// First allocator defers +33 KiB; the alloc side holds 33 KiB <= 40 KiB.
        EXPECT_TRUE(memory.sync(33 * 1024, allocating1));
        /// Second allocator would push the alloc side to 66 KiB > 40 KiB and must flush — the deferred
        /// free does not offset it. With a single signed counter the net would be -33+33+33 = +33 KiB
        /// and this would have been wrongly admitted.
        EXPECT_FALSE(memory.sync(33 * 1024, allocating2));
        memory.release(allocating2);   /// false sync -> caller releases (see CurrentMemoryTracker)
        EXPECT_EQ(allocating2.contributed, 0);
        EXPECT_EQ(memory.allocatedOnCPU(cpu), 33 * 1024);
        EXPECT_EQ(memory.freedOnCPU(cpu), -33 * 1024);

        /// The freeing thread flushes/exits: only the free side drains, the alloc side is untouched —
        /// the admitted allocation is not stranded against a counter that just dropped.
        memory.release(freeing);
        EXPECT_EQ(memory.allocatedOnCPU(cpu), 33 * 1024);
        EXPECT_EQ(memory.freedOnCPU(cpu), 0);
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// A failed flush must restore the snapshotted contribution unconditionally — even if the budget
/// was consumed by another thread in the window — because the bytes are already allocated and
/// still live. A gated sync would refuse and silently drop them, leaving them uncounted.
TEST(PerCPUMemory, RollbackRestoresContributionEvenWhenBudgetFull)
{
    const bool ran = runOnAnyCpu([&](int cpu)
    {
        const Int64 capacity = 64 * 1024;
        PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), capacity, /*buffer*/ 32 * 1024);

        PerCPUMemoryThreadState mine;
        EXPECT_TRUE(memory.sync(60 * 1024, mine));
        const PerCPUMemoryThreadState snapshot = mine;

        /// Flush path: our contribution is released, then another thread fills the CPU budget.
        memory.release(mine);
        EXPECT_EQ(memory.allocatedOnCPU(cpu), 0);
        PerCPUMemoryThreadState other;
        EXPECT_TRUE(memory.sync(capacity, other));
        EXPECT_EQ(memory.allocatedOnCPU(cpu), capacity);

        /// Rollback restores our 60 KiB despite the budget being full — a transient overshoot, not a drop.
        memory.rollback(mine, snapshot);
        EXPECT_EQ(mine.contributed, 60 * 1024);
        EXPECT_EQ(mine.contributed_on_cpu, cpu);
        EXPECT_EQ(memory.allocatedOnCPU(cpu), capacity + 60 * 1024);
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// Once the CPU's net leaves the budget the next thread is told to flush and its contribution is
/// backed out — both directions.
TEST(PerCPUMemory, NetOutOfBudgetForcesFlush)
{
    const bool ran = runOnAnyCpu([&](int cpu)
    {
        const Int64 capacity = 128 * 1024;
        PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), capacity, 32 * 1024);

        /// Over-commit direction.
        PerCPUMemoryThreadState a;
        PerCPUMemoryThreadState b;
        EXPECT_TRUE(memory.sync(100 * 1024, a));        /// net 100 KiB <= 128 KiB
        EXPECT_FALSE(memory.sync(100 * 1024, b));       /// would be 200 KiB > 128 KiB -> flush
        memory.release(b);                                 /// false sync -> caller releases
        EXPECT_EQ(b.contributed, 0);                       /// b's contribution backed out
        EXPECT_EQ(memory.netOnCPU(cpu), 100 * 1024);       /// only a remains

        /// Over-report direction (symmetric).
        PerCPUMemory negative(PerCPUMemory::numberOfCPUs(), capacity, 32 * 1024);
        PerCPUMemoryThreadState c;
        PerCPUMemoryThreadState d;
        EXPECT_TRUE(negative.sync(-100 * 1024, c));
        EXPECT_FALSE(negative.sync(-100 * 1024, d));
        negative.release(d);                               /// false sync -> caller releases
        EXPECT_EQ(d.contributed, 0);
        EXPECT_EQ(negative.netOnCPU(cpu), -100 * 1024);
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// Migration moves a thread's contribution from the old CPU's counter to the new one — the old
/// CPU is drained and the new one loaded, with no leak.
TEST(PerCPUMemory, MigrationMovesContribution)
{
    const auto cpus = allowedCpus();
    if (cpus.size() < 2)
        GTEST_SKIP() << "need at least 2 CPUs";
    const int cpu_a = cpus[0];
    const int cpu_b = cpus[1];

    PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), 8 * 1024 * 1024, 32 * 1024);
    PerCPUMemoryThreadState state;

    const bool ran =
        runOnCpu(cpu_a, [&]
        {
            EXPECT_TRUE(memory.sync(100 * 1024, state));
            EXPECT_EQ(state.contributed_on_cpu, cpu_a);
            EXPECT_EQ(memory.netOnCPU(cpu_a), 100 * 1024);
        })
        && runOnCpu(cpu_b, [&]
        {
            EXPECT_TRUE(memory.sync(140 * 1024, state));    /// drift >= buffer triggers the fold; CPU changed
            EXPECT_EQ(state.contributed_on_cpu, cpu_b);
            EXPECT_EQ(state.contributed, 140 * 1024);
            EXPECT_EQ(memory.netOnCPU(cpu_a), 0);              /// drained off the old CPU
            EXPECT_EQ(memory.netOnCPU(cpu_b), 140 * 1024);     /// folded onto the new CPU
        });
    if (!ran)
        GTEST_SKIP() << "cannot pin CPU";

    /// release() uses state.contributed_on_cpu, not sched_getcpu(), so it is correct unpinned.
    memory.release(state);
    EXPECT_EQ(memory.netOnCPU(cpu_b), 0);
}

/// capacity == 0 disables the per-CPU bound: sync never forces a flush.
TEST(PerCPUMemory, ZeroBudgetDisablesPerCpuBound)
{
    const bool ran = runOnAnyCpu([&](int)
    {
        PerCPUMemory memory(PerCPUMemory::numberOfCPUs(), /*capacity*/ 0, /*buffer*/ 32 * 1024);
        PerCPUMemoryThreadState state;
        /// Far more than any real capacity; still no per-CPU flush.
        EXPECT_TRUE(memory.sync(1024 * 1024 * 1024, state));
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";
}

/// Integration: with a per-thread limit far above what we allocate, the per-CPU budget is the
/// only thing that can force a flush; it must still let the hard memory limit be enforced.
TEST(PerCPUMemory, PerCpuBudgetEnforcesLimitDespiteHugePerThreadLimit)
{
    resetTrackers();

    bool limit_hit = false;
    const bool ran = runOnAnyCpu([&](int)
    {
        auto & thread = CurrentThread::get();
        const Int64 saved_thread_limit = thread.untracked_memory_limit;
        const Int64 saved_hard_limit = total_memory_tracker.getHardLimit();

        per_cpu_memory.setBudgetCapacity(1 * 1024 * 1024);

        const Int64 hard_limit = 16 * 1024 * 1024;
        thread.untracked_memory_limit = 1LL << 30;
        total_memory_tracker.setHardLimit(hard_limit);

        const Int64 cap = hard_limit + 8 * 1024 * 1024;   /// terminate a never-flushing regression
        for (Int64 allocated = 0; allocated < cap; allocated += 256 * 1024)
        {
            try
            {
                std::ignore = CurrentMemoryTracker::alloc(256 * 1024);
            }
            catch (const DB::Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
                limit_hit = true;
                break;
            }
        }

        thread.untracked_memory_limit = saved_thread_limit;
        total_memory_tracker.setHardLimit(saved_hard_limit);
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";

    resetTrackers();
    EXPECT_TRUE(limit_hit);
}

/// After the refactor a false sync() leaves the contribution on the counter; the real caller
/// (CurrentMemoryTracker) must release() it on the forced flush. Verify the thread-local
/// contribution is cleared after a per-CPU-budget-forced flush.
TEST(PerCPUMemory, CallerReleasesContributionOnBudgetFlush)
{
    resetTrackers();

    const bool ran = runOnAnyCpu([&](int)
    {
        auto & thread = CurrentThread::get();
        const Int64 saved_thread_limit = thread.untracked_memory_limit;

        const Int64 budget = 256 * 1024;
        per_cpu_memory.setBudgetCapacity(budget);
        thread.untracked_memory_limit = 1LL << 30;   /// only the per-CPU budget can force a flush

        /// One allocation past the budget: sync() returns false, the caller must flush and release.
        std::ignore = CurrentMemoryTracker::allocNoThrow(budget + PerCPUMemory::DEFAULT_THREAD_BUFFER + 1);
        EXPECT_EQ(thread.per_cpu_untracked_memory.contributed, 0);

        thread.untracked_memory_limit = saved_thread_limit;
    });
    if (!ran)
        GTEST_SKIP() << "cannot pin a CPU";

    resetTrackers();
}
#endif

TEST(PerCPUMemory, BalancedAllocFreeDoesNotInflateTracker)
{
    resetTrackers();

    constexpr Int64 size = 16 * 1024 * 1024;
    const Int64 before = total_memory_tracker.get();

    std::ignore = CurrentMemoryTracker::allocNoThrow(size);
    CurrentThread::flushUntrackedMemory();
    std::ignore = CurrentMemoryTracker::free(size);
    CurrentThread::flushUntrackedMemory();

    EXPECT_EQ(total_memory_tracker.get() - before, 0);
}

TEST(PerCPUMemory, RepeatedAllocFreePairsDoNotDrift)
{
    resetTrackers();

    constexpr Int64 size = 8 * 1024 * 1024;
    constexpr int iterations = 64;
    const Int64 before = total_memory_tracker.get();

    for (int i = 0; i < iterations; ++i)
    {
        std::ignore = CurrentMemoryTracker::allocNoThrow(size);
        std::ignore = CurrentMemoryTracker::free(size);
    }
    CurrentThread::flushUntrackedMemory();

    EXPECT_EQ(total_memory_tracker.get() - before, 0);
}

/// Switching the tracker parent must give the switched scope its own per-CPU contribution and
/// restore the previous one afterwards.
TEST(PerCPUMemory, SwitcherPreservesPerCpuState)
{
    resetTrackers();

    auto & thread = CurrentThread::get();
    thread.per_cpu_untracked_memory.contributed = 64 * 1024;
    thread.per_cpu_untracked_memory.contributed_on_cpu = 0;

    MemoryTracker other{VariableContext::Process};
    {
        MemoryTrackerSwitcher switcher(&other);

        EXPECT_EQ(thread.per_cpu_untracked_memory.contributed, 0);
        EXPECT_EQ(thread.per_cpu_untracked_memory.contributed_on_cpu, -1);

        std::ignore = CurrentMemoryTracker::allocNoThrow(2 * 1024);
    }

    EXPECT_EQ(thread.per_cpu_untracked_memory.contributed, 64 * 1024);
    EXPECT_EQ(thread.per_cpu_untracked_memory.contributed_on_cpu, 0);

    /// Clear the synthetic contribution so it is not backed out of a real counter later.
    thread.per_cpu_untracked_memory = {};
    resetTrackers();
}

TEST(PerCPUMemory, ConcurrentAllocFreeAcrossThreadsDoesNotDrift)
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

    const Int64 slack = 4096;
    const Int64 drift = total_memory_tracker.get() - before;
    EXPECT_GE(drift, -slack);
    EXPECT_LE(drift, slack);
}
