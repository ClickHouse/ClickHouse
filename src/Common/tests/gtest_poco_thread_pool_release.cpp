#include <gtest/gtest.h>

#if defined(OS_LINUX)

#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <ctime>
#include <latch>
#include <thread>
#include <pthread.h>

namespace
{

std::atomic<bool> stall_started = false;
std::atomic<bool> release_timed_out = false;
std::atomic<bool> stall_finished = false;

/// Holds the thread until the pool's 10 s wait for it has timed out, then one second longer.
void stallThread(int)
{
    int saved_errno = errno;
    stall_started = true;
    while (!release_timed_out)
    {
        timespec poll_interval{.tv_sec = 0, .tv_nsec = 10'000'000};
        nanosleep(&poll_interval, nullptr);
    }
    timespec remaining{.tv_sec = 1, .tv_nsec = 0};
    while (nanosleep(&remaining, &remaining) == -1 && errno == EINTR)
    {
    }
    stall_finished = true;
    errno = saved_errno;
}

struct Task : Poco::Runnable
{
    explicit Task(std::latch & started_) : started(started_) {}

    void run() override
    {
        thread = pthread_self();
        started.arrive_and_wait();
    }

    std::latch & started;
    pthread_t thread{};
};

}

TEST(PocoThreadPool, StalledReleasedThreadIsJoinedLater)
{
    stall_started = false;
    release_timed_out = false;
    stall_finished = false;

    const int signal_number = SIGRTMIN + 5;
    struct sigaction action{};
    struct sigaction old_action{};
    action.sa_handler = stallThread;
    sigemptyset(&action.sa_mask);
    ASSERT_EQ(sigaction(signal_number, &action, &old_action), 0);

    {
        std::latch started(3);
        Task kept(started);
        Task released(started);
        Poco::ThreadPool pool(/* minCapacity */ 1, /* maxCapacity */ 2, /* idleTime */ 1);
        pool.start(kept);
        pool.start(released);
        started.arrive_and_wait();
        while (pool.used() != 0)
            std::this_thread::yield();

        /// Not `joinAll`: its `housekeep` would release the second thread before it is stalled.
        /// `idleTime` has a resolution of one second.
        std::this_thread::sleep_for(std::chrono::seconds(2));

        ASSERT_EQ(pthread_kill(released.thread, signal_number), 0);
        while (!stall_started)
            std::this_thread::yield();

        /// Keeps the first thread for `minCapacity` and stops waiting for the stalled second one after 10 s.
        pool.collect();
        EXPECT_EQ(pool.allocated(), 1);
        release_timed_out = true;
    }
    EXPECT_TRUE(stall_finished);

    sigaction(signal_number, &old_action, nullptr);
}

#endif
