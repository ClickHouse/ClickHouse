#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>


namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace
{

bool waitFor(const std::atomic<bool> & flag)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!flag.load())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

}

/// A job scheduled through `scheduleThreadOrThrow` occupies its worker for the worker's whole
/// lifetime, so the scheduler hands it to a specific worker instead of putting it into the shared
/// queue. When the pool is at `max_threads` and has an idle worker, that worker must be the one to
/// run the job even if a higher-priority ordinary job is scheduled right after it: with the job in
/// the shared queue the woken worker would take the ordinary job from the top of the heap, and the
/// caller of `scheduleThreadOrThrow` would be left with a "thread" whose function has not started.
TEST(ThreadPool, ThreadJobIsHandedToIdleWorkerDirectly)
{
    static constexpr size_t max_threads = 2;
    ThreadPool pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        max_threads, /* max_free_threads */ max_threads, /* queue_size */ 100);

    std::atomic<bool> blocker_started = false;
    std::atomic<bool> release_blocker = false;
    std::atomic<bool> warm_up_done = false;
    std::atomic<bool> thread_job_started = false;
    std::atomic<bool> release_thread_job = false;
    std::atomic<bool> ordinary_job_ran = false;

    /// Worker 1: busy with an ordinary job for the whole test.
    pool.scheduleOrThrow([&]
    {
        blocker_started = true;
        while (!release_blocker.load())
            std::this_thread::yield();
    });
    ASSERT_TRUE(waitFor(blocker_started));

    /// Worker 2: created for a short job, then idle (kept by `max_free_threads`).
    pool.scheduleOrThrow([&] { warm_up_done = true; });
    ASSERT_TRUE(waitFor(warm_up_done));
    /// The worker pushes itself into the idle stack in the same critical section in which it
    /// accounts for the finished job, so once `active` drops to 1 it is idle and at `max_threads`
    /// no fresh worker can be started for the next job.
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (pool.active() != 1)
        {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline);
            std::this_thread::yield();
        }
    }

    /// The long-lived job must go to worker 2 right away...
    pool.scheduleThreadOrThrow([&]
    {
        thread_job_started = true;
        while (!release_thread_job.load())
            std::this_thread::yield();
    });
    /// ...even though a higher-priority ordinary job is scheduled immediately after it.
    pool.scheduleOrThrow([&] { ordinary_job_ran = true; }, Priority{-1});

    EXPECT_TRUE(waitFor(thread_job_started)) << "the long-lived job was left in the queue behind the ordinary job";
    /// Both workers are held now: the ordinary job has to wait for the blocker.
    EXPECT_FALSE(ordinary_job_ran);

    release_blocker = true;
    EXPECT_TRUE(waitFor(ordinary_job_ran));

    release_thread_job = true;
    pool.wait();
}

/// The counters of the pool may say that there are free workers while none of them is in the idle
/// stack: a worker that was just woken by a concurrent `schedule`, and may find no job for itself, is
/// not linked into the stack until it reacquires the mutex. `scheduleThreadOrThrow` used to refuse such
/// a job with "all threads of the pool are busy" although the pool was far below `max_threads`. It must
/// start a fresh worker instead. The jobs here are short and never hold more than a few workers at once,
/// so none of them may be refused.
TEST(ThreadPool, ThreadJobIsNotRefusedWhileBelowMaxThreads)
{
    static constexpr size_t max_threads = 256;
    static constexpr size_t schedulers = 4;
    static constexpr size_t iterations = 5000;

    ThreadPool pool(
        CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
        max_threads, /* max_free_threads */ max_threads, /* queue_size */ 0);

    std::atomic<size_t> refused = 0;
    std::atomic<size_t> jobs_done = 0;

    std::vector<std::thread> threads;
    threads.reserve(schedulers);
    for (size_t i = 0; i < schedulers; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < iterations; ++j)
            {
                pool.scheduleOrThrow([&] { ++jobs_done; });
                try
                {
                    pool.scheduleThreadOrThrow([&] { ++jobs_done; });
                }
                catch (const DB::Exception &)
                {
                    ++refused;
                }
            }
        });
    }

    for (auto & thread : threads)
        thread.join();
    pool.wait();

    EXPECT_EQ(refused.load(), 0);
    EXPECT_EQ(jobs_done.load(), 2 * schedulers * iterations - refused.load());
}
