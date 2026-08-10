#include <atomic>
#include <optional>

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

/// Test what happens if all the threads of the global pool are busy with *ordinary* jobs (jobs that
/// return, unlike the `ThreadFromGlobalPool` workers of `ThreadPool.GlobalFullLargeQueue`) while its
/// queue still has room.
///
/// Reserving one of the `max_threads` slots for every `ThreadFromGlobalPool` at scheduling time is not
/// enough on its own: the slots may be free while every worker is occupied by an ordinary job. If one
/// of those jobs is the one waiting for the new thread to make progress, queueing that thread has the
/// same effect as before - its creator believes the thread is running, but its function has not even
/// started, and nothing will ever free a worker. So scheduling a thread must fail when the pool is at
/// `max_threads` and none of its workers can take the job right away.

TEST(ThreadPool, GlobalBusyWithOrdinaryJobs)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    /// The queue is much larger than the number of threads, so a job that cannot get a thread would
    /// be accepted into the queue rather than rejected.
    global_pool.setQueueSize(1000);

    /// `ThreadFromGlobalPool`s of the local pools from the previous test cases may not have exited yet.
    global_pool.wait();

    std::atomic<size_t> started = 0;
    std::atomic<size_t> finished = 0;
    std::atomic<bool> release = false;

    /// An ordinary job: it returns, so it does not hold a worker slot for the whole lifetime of its
    /// worker, but it does keep the worker busy while it runs.
    auto occupy_thread = [&] { ++started; while (!release.load()) {} ++finished; };

    for (size_t i = 0; i < capacity; ++i)
        global_pool.scheduleOrThrow(occupy_thread);

    /// All the threads of the global pool are busy now.
    while (started != capacity)
        ;

    /// This creates a `ThreadFromGlobalPool` for the worker of the local pool. There is no thread to
    /// run it, so it has to fail instead of being parked in the queue behind the jobs above.
    std::optional<ThreadPool> another_pool(
        std::in_place,
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1);
    bool threw = false;
    try
    {
        another_pool->scheduleOrThrowOnError([] {});
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }
    EXPECT_TRUE(threw) << "scheduling a thread while all the threads of the global pool are busy must throw";

    /// Let the occupying jobs finish. Do it before destroying `another_pool`: on the unfixed pool its
    /// worker is sitting in the global queue, and joining a "thread" whose function never started is
    /// the very hang this test covers. Waiting for the jobs with `global_pool.wait()` is not an option
    /// either, because on the unfixed pool the queued worker of `another_pool` is a job that does not
    /// return until that pool is destroyed.
    release = true;
    while (finished != capacity)
        ;

    another_pool.reset();

    /// Now no job of this test is left in the global pool.
    global_pool.wait();

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}
