#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

TEST(ThreadPool, LIFOStartsManyBlockingJobs)
{
    constexpr size_t jobs = 64;
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, jobs, jobs, jobs);

    std::mutex mutex;
    std::condition_variable cv;
    size_t started = 0;
    bool release = false;

    for (size_t i = 0; i < jobs; ++i)
    {
        pool.scheduleOrThrowOnError([&]
        {
            std::unique_lock lock(mutex);
            ++started;
            cv.notify_all();
            cv.wait(lock, [&]
            {
                return release;
            });
        });
    }

    bool all_started = false;
    {
        std::unique_lock lock(mutex);
        all_started = cv.wait_for(lock, std::chrono::seconds(30), [&]
        {
            return started == jobs;
        });
        release = true;
    }
    cv.notify_all();

    pool.wait();

    EXPECT_TRUE(all_started) << "Only " << started << " jobs started";
}

TEST(ThreadPool, LIFONotifyWithThreadSelfRemoval)
{
    constexpr size_t jobs = 128;

    for (size_t iteration = 0; iteration < 100; ++iteration)
    {
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 8, 0, jobs);

        std::atomic<size_t> counter = 0;
        for (size_t i = 0; i < jobs; ++i)
        {
            pool.scheduleOrThrowOnError([&]
            {
                ++counter;
            });
        }

        pool.wait();
        EXPECT_EQ(counter.load(), jobs);
    }
}

/// Verify the LIFO contract directly: with a known idle order, the next
/// scheduled job runs on the most recently idle worker thread.
TEST(ThreadPool, LIFOSchedulesOnMostRecentlyIdleThread)
{
    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 2, 2, 2);

    std::mutex mutex;
    std::condition_variable cv;
    bool release_a = false;
    bool release_b = false;
    bool a_started = false;
    bool b_started = false;
    std::thread::id thread_a;
    std::thread::id thread_b;

    /// Start two blocking jobs that occupy both worker threads.
    pool.scheduleOrThrowOnError([&]
    {
        std::unique_lock lock(mutex);
        thread_a = std::this_thread::get_id();
        a_started = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_a; });
    });

    pool.scheduleOrThrowOnError([&]
    {
        std::unique_lock lock(mutex);
        thread_b = std::this_thread::get_id();
        b_started = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_b; });
    });

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return a_started && b_started; });
    }

    ASSERT_NE(thread_a, thread_b);

    /// Release the first job. Its worker becomes the only idle thread.
    {
        std::lock_guard lock(mutex);
        release_a = true;
    }
    cv.notify_all();

    /// Probe with a quick job. With job B still running, only `thread_a` can
    /// run the probe, regardless of whether it has reached the idle stack at
    /// the moment of scheduling: if not yet idle, the probe is queued and
    /// `thread_a` picks it up when it returns to the worker loop.
    std::thread::id probe_x_thread;
    bool probe_x_done = false;
    pool.scheduleOrThrowOnError([&]
    {
        std::unique_lock lock(mutex);
        probe_x_thread = std::this_thread::get_id();
        probe_x_done = true;
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return probe_x_done; });
    }
    EXPECT_EQ(probe_x_thread, thread_a);

    /// Release the second job; its worker becomes idle and is pushed onto the
    /// LIFO stack after `thread_a`, so `thread_b` is now the most recently idle.
    {
        std::lock_guard lock(mutex);
        release_b = true;
    }
    cv.notify_all();

    /// `pool.wait` returns only after the worker that completed the last job
    /// has pushed itself onto the idle stack: the worker holds `pool.mutex`
    /// continuously from notifying `job_finished` through the idle push, and
    /// `pool.wait` reacquires that mutex before returning.
    pool.wait();

    /// Now both threads are idle. The LIFO contract requires that this probe
    /// runs on `thread_b`, the most recently idle worker.
    std::thread::id probe_y_thread;
    bool probe_y_done = false;
    pool.scheduleOrThrowOnError([&]
    {
        std::unique_lock lock(mutex);
        probe_y_thread = std::this_thread::get_id();
        probe_y_done = true;
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return probe_y_done; });
    }
    EXPECT_EQ(probe_y_thread, thread_b);

    pool.wait();
}

/// Shrinking the pool via `setMaxThreads` must retire excess threads without
/// collapsing the surviving idle workers. After the shrink, subsequent jobs
/// must run only on a subset of the original threads (no new threads), and
/// at most `shrunk_threads` distinct workers should be observed.
TEST(ThreadPool, ShrinkMaxThreadsRetiresExcessWithoutCollapsingIdlePool)
{
    constexpr size_t initial_threads = 8;
    constexpr size_t shrunk_threads = 2;

    ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, initial_threads, initial_threads, initial_threads);

    /// Force the pool to create all `initial_threads` workers by running that
    /// many simultaneously blocking jobs.
    std::mutex mutex;
    std::condition_variable cv;
    size_t started = 0;
    bool release = false;
    std::vector<std::thread::id> original_thread_ids;
    original_thread_ids.reserve(initial_threads);

    for (size_t i = 0; i < initial_threads; ++i)
    {
        pool.scheduleOrThrowOnError([&]
        {
            std::unique_lock lock(mutex);
            original_thread_ids.push_back(std::this_thread::get_id());
            ++started;
            cv.notify_all();
            cv.wait(lock, [&] { return release; });
        });
    }

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return started == initial_threads; });
    }

    ASSERT_EQ(original_thread_ids.size(), initial_threads);
    std::set<std::thread::id> original_ids(original_thread_ids.begin(), original_thread_ids.end());
    ASSERT_EQ(original_ids.size(), initial_threads);

    {
        std::lock_guard lock(mutex);
        release = true;
    }
    cv.notify_all();
    pool.wait();

    /// At this point, all `initial_threads` workers are linked into the LIFO
    /// idle stack. `setMaxThreads` should pop and wake exactly the excess
    /// (oldest) threads; the surviving (newest) threads remain on the stack.
    pool.setMaxThreads(shrunk_threads);
    EXPECT_EQ(pool.getMaxThreads(), shrunk_threads);

    /// Run many sequential jobs and capture which thread runs each one. None
    /// of these jobs should be assigned to a thread that was popped during
    /// the shrink (those threads are no longer in the idle stack and exit on
    /// their own once they observe the new `max_threads`).
    constexpr size_t probes = 200;
    std::set<std::thread::id> post_shrink_ids;
    for (size_t i = 0; i < probes; ++i)
    {
        std::mutex local_mutex;
        std::condition_variable local_cv;
        std::thread::id id;
        bool done = false;
        pool.scheduleOrThrowOnError([&]
        {
            std::unique_lock lock(local_mutex);
            id = std::this_thread::get_id();
            done = true;
            local_cv.notify_all();
        });
        {
            std::unique_lock lock(local_mutex);
            local_cv.wait(lock, [&] { return done; });
        }
        EXPECT_TRUE(original_ids.contains(id)) << "Post-shrink job ran on a thread that did not exist before the shrink";
        post_shrink_ids.insert(id);
    }

    EXPECT_LE(post_shrink_ids.size(), shrunk_threads)
        << "Post-shrink jobs used " << post_shrink_ids.size() << " distinct threads, expected at most " << shrunk_threads;

    pool.wait();
}
