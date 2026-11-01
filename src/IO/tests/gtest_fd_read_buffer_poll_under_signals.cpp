#include <gtest/gtest.h>

#if defined(OS_LINUX)

/// glibc defines `sa_handler` (and musl `sigev_notify_thread_id`) as self-referential macros.
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <csignal>
#include <ctime>
#include <unistd.h>

#include <base/getThreadId.h>
#include <Common/Epoll.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFileDescriptor.h>

namespace
{

void onTick(int)
{
}

/// poll must expire after the requested timeout even when the thread receives periodic
/// signals: restarting the interrupted poll with the full timeout would reset the deadline
/// on every signal, making the wait unbounded while the fd stays silent. The query profiler
/// delivers such signals to every query thread (its SA_RESTART does not apply to poll).
/// The sub-millisecond cadence additionally verifies that the remaining time is accounted
/// with enough precision: per-retry whole-millisecond accounting would truncate each
/// interval to zero and never make progress.
///
/// The signal source is a per-thread kernel timer (SIGEV_THREAD_ID), exactly like the query
/// profiler: delivery targets the polling thread deterministically (a process-directed signal
/// could be consumed by any other unblocked thread of the test binary) and keeps the requested
/// cadence at kernel-timer precision (a userspace ticker thread cannot sustain a reliable
/// sub-millisecond period).
/// Runs `wait` on a thread receiving a periodic SIGALRM (via a per-thread kernel timer, like the
/// query profiler) while a pipe read fd stays silent, and returns how long `wait` blocked in
/// milliseconds. `wait(read_fd)` performs the blocking wait with a 200 ms timeout and returns its
/// own elapsed time. The timer is disarmed after at most 3 s so a build that never expires (the
/// pre-fix behavior) fails the caller's assertion instead of hanging the test binary.
template <typename WaitFn>
UInt64 measureSilentWaitUnderPeriodicSignals(Int64 signal_period_nanoseconds, WaitFn && wait)
{
    int fds[2];
    EXPECT_EQ(pipe(fds), 0);

    struct sigaction sa = {};
    sa.sa_handler = onTick; /// interrupts the wait with EINTR (SA_RESTART would not help poll/epoll_wait either)
    struct sigaction old_sa = {};
    EXPECT_EQ(sigaction(SIGALRM, &sa, &old_sa), 0);

    struct sigevent sev {};
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = SIGALRM;
#if defined(USE_MUSL)
    sev.sigev_notify_thread_id = static_cast<pid_t>(getThreadId());
#else
    sev._sigev_un._tid = static_cast<pid_t>(getThreadId());
#endif
    timer_t timer_id = nullptr;
    EXPECT_EQ(timer_create(CLOCK_MONOTONIC, &sev, &timer_id), 0);

    itimerspec period = {{0, signal_period_nanoseconds}, {0, signal_period_nanoseconds}};
    EXPECT_EQ(timer_settime(timer_id, 0, &period, nullptr), 0);

    std::mutex mutex;
    std::condition_variable cv;
    bool wait_returned = false;
    std::thread disarm(
        [&]
        {
            std::unique_lock lock(mutex);
            cv.wait_for(lock, std::chrono::seconds(3), [&] { return wait_returned; });
            itimerspec stop = {};
            timer_settime(timer_id, 0, &stop, nullptr);
        });

    const UInt64 elapsed_ms = wait(fds[0]);

    {
        std::lock_guard lock(mutex);
        wait_returned = true;
    }
    cv.notify_one();
    disarm.join();

    timer_delete(timer_id);
    sigaction(SIGALRM, &old_sa, nullptr);

    close(fds[0]);
    close(fds[1]);

    return elapsed_ms;
}

void checkPollExpiresUnderPeriodicSignals(Int64 signal_period_nanoseconds)
{
    const UInt64 elapsed_ms = measureSilentWaitUnderPeriodicSignals(
        signal_period_nanoseconds,
        [](int read_fd) -> UInt64
        {
            DB::ReadBufferFromFileDescriptor in(read_fd);
            Stopwatch watch;
            const bool has_data = in.poll(200'000); /// 200 ms
            const UInt64 wait_elapsed_ms = watch.elapsedMilliseconds();
            EXPECT_FALSE(has_data);
            return wait_elapsed_ms;
        });
    /// Fixed: ~200 ms. Broken (deadline reset by each signal): >= 3 s, until the timer is disarmed.
    EXPECT_LT(elapsed_ms, 1500);
}

void checkEpollExpiresUnderPeriodicSignals(Int64 signal_period_nanoseconds)
{
    const UInt64 elapsed_ms = measureSilentWaitUnderPeriodicSignals(
        signal_period_nanoseconds,
        [](int read_fd) -> UInt64
        {
            DB::Epoll epoll;
            epoll.add(read_fd);
            epoll_event events[1];
            Stopwatch watch;
            const size_t ready = epoll.getManyReady(1, events, 200); /// 200 ms
            const UInt64 wait_elapsed_ms = watch.elapsedMilliseconds();
            EXPECT_EQ(ready, 0u);
            return wait_elapsed_ms;
        });
    /// Fixed: ~200 ms. Broken (deadline reset by each signal): >= 3 s, until the timer is disarmed.
    EXPECT_LT(elapsed_ms, 1500);
}

}

TEST(ReadBufferFromFileDescriptor, PollExpiresUnderPeriodicSignals)
{
    checkPollExpiresUnderPeriodicSignals(10'000'000); /// 10 ms
}

TEST(ReadBufferFromFileDescriptor, PollExpiresUnderSubMillisecondSignals)
{
    checkPollExpiresUnderPeriodicSignals(500'000); /// 0.5 ms
}

TEST(Epoll, GetManyReadyExpiresUnderPeriodicSignals)
{
    checkEpollExpiresUnderPeriodicSignals(10'000'000); /// 10 ms
}

TEST(Epoll, GetManyReadyExpiresUnderSubMillisecondSignals)
{
    checkEpollExpiresUnderPeriodicSignals(500'000); /// 0.5 ms
}

TEST(ReadBufferFromFileDescriptor, ZeroTimeoutPollProbe)
{
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    {
        DB::ReadBufferFromFileDescriptor empty(fds[0]);
        EXPECT_FALSE(empty.poll(0)); /// non-blocking probe on an empty fd: not ready
        const char byte = 'x';
        ASSERT_EQ(write(fds[1], &byte, 1), 1);
        DB::ReadBufferFromFileDescriptor ready(fds[0]);
        EXPECT_TRUE(ready.poll(0)); /// non-blocking probe on a ready fd: ready
    }
    close(fds[0]);
    close(fds[1]);
}

TEST(Epoll, ZeroTimeoutGetManyReadyProbe)
{
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    {
        DB::Epoll epoll;
        epoll.add(fds[0]);
        epoll_event events[1];
        EXPECT_EQ(epoll.getManyReady(1, events, 0), 0u); /// non-blocking probe on an empty fd: nothing ready
        const char byte = 'x';
        ASSERT_EQ(write(fds[1], &byte, 1), 1);
        EXPECT_EQ(epoll.getManyReady(1, events, 0), 1u); /// non-blocking probe on a ready fd: one ready
    }
    close(fds[0]);
    close(fds[1]);
}

#endif
