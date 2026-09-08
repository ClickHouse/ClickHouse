#include <atomic>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
}

/// Test what happens if local ThreadPool cannot create a ThreadFromGlobalPool.
/// There was a bug: if local ThreadPool cannot allocate even a single thread,
///  the job will be scheduled but never get executed.

namespace
{

/// Fault injection is process-global, so it has to be switched off on every path out of the
/// scope, including an exception or a fatal assertion: while it is on, no thread pool anywhere
/// in this binary can start a thread.
struct AlwaysFailToAllocateThread
{
    AlwaysFailToAllocateThread() { CannotAllocateThreadFaultInjector::setFaultProbability(1.0); }
    ~AlwaysFailToAllocateThread() { CannotAllocateThreadFaultInjector::setFaultProbability(0.0); }
};

/// A pool refuses a job either because it could not start a thread or because its queue would
/// not admit it. Both refusals carry `CANNOT_SCHEDULE_TASK`, so the reason string is the only
/// thing that distinguishes them, and only the first one is what these tests are about.
void expectFailureToStartThread(ThreadPool & pool, ThreadPool::Job job)
{
    bool threw = false;
    try
    {
        pool.scheduleOrThrowOnError(std::move(job));
    }
    catch (const DB::Exception & e)
    {
        threw = true;
        EXPECT_EQ(e.code(), DB::ErrorCodes::CANNOT_SCHEDULE_TASK);
        EXPECT_TRUE(e.message().contains("failed to start the thread")) << e.message();
    }
    EXPECT_TRUE(threw);
}

}


TEST(ThreadPool, GlobalFull1)
{
    static constexpr size_t capacity = 5;

    std::atomic<size_t> counter = 0;
    static constexpr size_t num_jobs = capacity + 1;

    /// The counter only ever grows, and an unexpectedly admitted job pushes it past the target,
    /// so the predicate has to be an inequality for the spin to terminate at all.
    auto func = [&] { ++counter; while (counter < num_jobs) {} };

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_jobs);

    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    for (size_t i = capacity; i < num_jobs; ++i)
    {
        AlwaysFailToAllocateThread always_fail;
        expectFailureToStartThread(pool, func);
        ++counter;
    }

    pool.wait();
    EXPECT_EQ(counter, num_jobs);
}


TEST(ThreadPool, GlobalFull2)
{
    static constexpr size_t capacity = 5;

    std::atomic<size_t> counter = 0;
    auto func = [&] { ++counter; while (counter < capacity + 1) {} };

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, capacity, 0, capacity);
    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    ThreadPool another_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    {
        AlwaysFailToAllocateThread always_fail;
        expectFailureToStartThread(another_pool, func);
    }

    ++counter;

    pool.wait();

    /// Injection has to be off by this point, otherwise these jobs are rejected too and the
    /// pool never gets to demonstrate that it recovers.
    for (size_t i = 0; i < capacity; ++i)
        another_pool.scheduleOrThrowOnError([&] { ++counter; });

    another_pool.wait();
    EXPECT_EQ(counter, capacity * 2 + 1);
}
