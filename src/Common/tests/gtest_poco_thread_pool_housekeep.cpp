#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <latch>
#include <mutex>
#include <thread>

#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>

namespace
{

std::mutex mutex;
std::condition_variable released_cv;
bool released = false;
std::atomic<bool> exit_timed_out = false;
std::atomic<int> exited_threads = 0;

/// Destroyed while its thread exits, after `run` returned, so joining the thread waits for the destructor.
struct SlowThreadExit
{
    ~SlowThreadExit()
    {
        std::unique_lock lock(mutex);
        if (!released_cv.wait_for(lock, std::chrono::seconds(5), [] { return released; }))
            exit_timed_out = true;
        ++exited_threads;
    }
};

struct Task : Poco::Runnable
{
    explicit Task(std::latch & all_started_, bool stay_busy_ = false) : all_started(all_started_), stay_busy(stay_busy_) {}

    void run() override
    {
        thread_local SlowThreadExit slow_thread_exit;
        all_started.arrive_and_wait();
        if (stay_busy)
        {
            std::unique_lock lock(mutex);
            released_cv.wait(lock, [] { return released; });
        }
    }

    std::latch & all_started;
    bool stay_busy;
};

}

/// `getThread` runs `housekeep` from the accept loops of servers, so it must not wait for retired threads to exit.
TEST(PocoThreadPool, HousekeepDoesNotWaitForRetiredThreads)
{
    released = false;
    exit_timed_out = false;
    exited_threads = 0;
    {
        std::latch all_started(4);
        Task task(all_started);
        Poco::ThreadPool pool(/* minCapacity */ 1, /* maxCapacity */ 3, /* idleTime */ 1);
        for (int i = 0; i < 3; ++i)
            pool.start(task);
        /// Not `joinAll`: its `housekeep` would already retire threads that went idle just before a second boundary.
        all_started.arrive_and_wait();

        /// `idleTime` has a resolution of one second.
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pool.collect();
        {
            std::lock_guard lock(mutex);
            released = true;
        }
        released_cv.notify_all();

        /// One retired thread was joined by another, which stays in the pool with the one kept by `minCapacity`.
        pool.joinAll();
        EXPECT_EQ(exited_threads, 1);
        EXPECT_EQ(pool.allocated(), 2);
    }
    EXPECT_FALSE(exit_timed_out);
    EXPECT_EQ(exited_threads, 3);
}

/// Destroying the pool right after `housekeep` must not cancel the reaper, or the retired threads never exit.
TEST(PocoThreadPool, StopAllDoesNotCancelThreadReaper)
{
    released = false;
    exit_timed_out = false;
    exited_threads = 0;
    {
        std::latch all_started(4);
        Task task(all_started);
        Task busy_task(all_started, /* stay_busy */ true);
        Poco::ThreadPool pool(/* minCapacity */ 1, /* maxCapacity */ 3, /* idleTime */ 1);
        pool.start(task);
        pool.start(task);
        pool.start(busy_task);
        all_started.arrive_and_wait();

        /// With a busy thread, `housekeep` keeps only the reaper before it, so `stopAll` releases the reaper first.
        std::this_thread::sleep_for(std::chrono::seconds(2));
        pool.collect();
        {
            std::lock_guard lock(mutex);
            released = true;
        }
        released_cv.notify_all();
    }
    EXPECT_FALSE(exit_timed_out);
    EXPECT_EQ(exited_threads, 3);
}
