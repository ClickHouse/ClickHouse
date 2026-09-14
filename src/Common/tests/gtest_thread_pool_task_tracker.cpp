#include <Common/ThreadPoolTaskTracker.h>
#include <Common/setThreadName.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

using namespace DB;

namespace
{

LogSeriesLimiterPtr makeTestLogger()
{
    return std::make_shared<LogSeriesLimiter>(getLogger("TaskTrackerTest"), 1, 5);
}

struct AsyncTracker
{
    ThreadPool pool;
    TaskTracker tracker;

    explicit AsyncTracker(size_t threads, size_t max_inflight = 0)
        : pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, threads)
        , tracker(threadPoolCallbackRunnerUnsafe<void>(pool, ThreadName::UNKNOWN), max_inflight, makeTestLogger())
    {}
};

}

TEST(TaskTrackerAddFinal, AsyncHappyPathRunsFinalAfterAllPriors)
{
    AsyncTracker t{/*threads=*/4};
    constexpr size_t N = 16;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};
    std::atomic<size_t> count_observed_by_final{0};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            ran.fetch_add(1);
        });

    t.tracker.addFinal([&] {
        count_observed_by_final = ran.load();
        final_ran = true;
    });

    t.tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(count_observed_by_final.load(), N);
}

TEST(TaskTrackerAddFinal, AsyncZeroPriorsStillRunsFinal)
{
    AsyncTracker t{/*threads=*/2};
    std::atomic<bool> final_ran{false};

    t.tracker.addFinal([&] { final_ran = true; });
    t.tracker.waitAll();

    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncAllPriorsAlreadyDoneBeforeAddFinal)
{
    AsyncTracker t{/*threads=*/2};
    constexpr size_t N = 4;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] { ran.fetch_add(1); });

    /// Give workers a chance to finish before we call addFinal — exercises the path where
    /// `finished_futures.size() == futures.size()` is already true at the time addFinal is called.
    while (ran.load() != N)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    t.tracker.addFinal([&] { final_ran = true; });
    t.tracker.waitAll();

    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncPriorFailureDoesNotDeadlock)
{
    AsyncTracker t{/*threads=*/2};

    t.tracker.add([] { /* ok */ });
    t.tracker.add([] { throw std::runtime_error("boom"); });
    t.tracker.add([] { /* ok */ });
    t.tracker.addFinal([] {});

    EXPECT_THROW(t.tracker.waitAll(), std::runtime_error);
    /// The final callback is best-effort on failures — we only assert no deadlock and clean shutdown.
    t.tracker.safeWaitAll();
}

TEST(TaskTrackerAddFinal, AsyncFinalCallbackException)
{
    AsyncTracker t{/*threads=*/2};
    t.tracker.add([] { /* ok */ });
    t.tracker.addFinal([] { throw std::runtime_error("final boom"); });

    EXPECT_THROW(t.tracker.waitAll(), std::runtime_error);
    t.tracker.safeWaitAll();
}

TEST(TaskTrackerAddFinal, WaitIfAnyBeforeFinalTask)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_FALSE(tracker.isAsync());

    constexpr size_t N = 2;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    /// Sync runner has already run every task body and populated `finished_futures`.
    ASSERT_EQ(ran.load(), N);

    tracker.waitIfAny();

    tracker.addFinal([&] { final_ran = true; });

    tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
}

TEST(TaskTrackerAddFinal, AsyncWithInflightLimit)
{
    /// Exercise the path where `waitTilInflightShrink` is hit on the owner thread between adds.
    AsyncTracker t{/*threads=*/4, /*max_inflight=*/3};
    constexpr size_t N = 32;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};

    for (size_t i = 0; i < N; ++i)
        t.tracker.add([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            ran.fetch_add(1);
        });

    size_t observed_in_final = 0;
    t.tracker.addFinal([&] {
        observed_in_final = ran.load();
        final_ran = true;
    });
    t.tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(observed_in_final, N);
}

TEST(TaskTrackerAddFinal, SyncRunnerHappyPath)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());
    ASSERT_FALSE(tracker.isAsync());

    constexpr size_t N = 5;
    std::atomic<size_t> ran{0};
    std::atomic<bool> final_ran{false};
    size_t observed_in_final = 0;

    for (size_t i = 0; i < N; ++i)
        tracker.add([&] { ran.fetch_add(1); });

    tracker.addFinal([&] {
        observed_in_final = ran.load();
        final_ran = true;
    });
    tracker.waitAll();

    EXPECT_EQ(ran.load(), N);
    EXPECT_TRUE(final_ran.load());
    EXPECT_EQ(observed_in_final, N);
}

TEST(TaskTrackerAddFinal, SyncRunnerPriorFailureDoesNotDeadlock)
{
    TaskTracker tracker(/*scheduler_=*/{}, /*max_tasks_inflight=*/0, makeTestLogger());

    tracker.add([] { /* ok */ });
    tracker.add([] { throw std::runtime_error("boom"); });
    tracker.addFinal([] {});

    EXPECT_THROW(tracker.waitAll(), std::runtime_error);
    tracker.safeWaitAll();
}
