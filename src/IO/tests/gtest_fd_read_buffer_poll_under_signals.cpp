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
void checkPollExpiresUnderPeriodicSignals(long signal_period_nanoseconds)
{
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);

    struct sigaction sa = {};
    sa.sa_handler = onTick; /// interrupts poll with EINTR (SA_RESTART would not help poll either)
    struct sigaction old_sa = {};
    ASSERT_EQ(sigaction(SIGALRM, &sa, &old_sa), 0);

    struct sigevent sev {};
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = SIGALRM;
#if defined(USE_MUSL)
    sev.sigev_notify_thread_id = static_cast<pid_t>(getThreadId());
#else
    sev._sigev_un._tid = static_cast<pid_t>(getThreadId());
#endif
    timer_t timer_id = nullptr;
    ASSERT_EQ(timer_create(CLOCK_MONOTONIC, &sev, &timer_id), 0);

    itimerspec period = {{0, signal_period_nanoseconds}, {0, signal_period_nanoseconds}};
    ASSERT_EQ(timer_settime(timer_id, 0, &period, nullptr), 0);

    /// Disarm the timer after at most 3 s even if the poll never expires (the pre-fix
    /// behavior), so a broken build fails the elapsed assertion below instead of hanging
    /// the test binary. The timer is thread-directed, so this thread never receives it.
    std::mutex mutex;
    std::condition_variable cv;
    bool poll_returned = false;
    std::thread disarm(
        [&]
        {
            std::unique_lock lock(mutex);
            cv.wait_for(lock, std::chrono::seconds(3), [&] { return poll_returned; });
            itimerspec stop = {};
            timer_settime(timer_id, 0, &stop, nullptr);
        });

    DB::ReadBufferFromFileDescriptor in(fds[0]);

    Stopwatch watch;
    const bool has_data = in.poll(200'000); /// 200 ms
    const UInt64 elapsed_ms = watch.elapsedMilliseconds();

    {
        std::lock_guard lock(mutex);
        poll_returned = true;
    }
    cv.notify_one();
    disarm.join();

    timer_delete(timer_id);
    sigaction(SIGALRM, &old_sa, nullptr);

    EXPECT_FALSE(has_data);
    /// Fixed: ~200 ms. Broken (deadline reset by each signal): >= 3 s, until the timer is disarmed.
    EXPECT_LT(elapsed_ms, 1500);

    close(fds[0]);
    close(fds[1]);
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

#endif
