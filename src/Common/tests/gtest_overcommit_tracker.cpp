#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <limits>
#include <thread>
#include <vector>

#include <Common/MemoryTracker.h>
#include <Common/OvercommitTracker.h>
#include <Interpreters/ProcessList.h>

using namespace std::chrono_literals;
using namespace DB;

template <typename BaseTracker>
struct OvercommitTrackerForTest : BaseTracker
{
    template <typename ...Ts>
    explicit OvercommitTrackerForTest(Ts && ...args)
        : BaseTracker(std::move(args)...)
    {}

    void setCandidate(MemoryTracker * candidate)
    {
        tracker = candidate;
    }

    /// Ready once a query has been picked, which is the point from which a release can no longer be
    /// missed: the picking thread holds `overcommit_m` from here until it enters the wait.
    std::future<void> getSelectionFuture() { return query_selected.get_future(); }

protected:
    void pickQueryToExcludeImpl() override
    {
        BaseTracker::picked_tracker = tracker;
        /// One-shot: `reset` lets a tracker be picked again, and a second `set_value` would throw.
        if (!selection_signalled.exchange(true))
            query_selected.set_value();
    }

    MemoryTracker * tracker = nullptr;

private:
    std::promise<void> query_selected;
    std::atomic<bool> selection_signalled = false;
};

using UserOvercommitTrackerForTest = OvercommitTrackerForTest<UserOvercommitTracker>;
using GlobalOvercommitTrackerForTest = OvercommitTrackerForTest<GlobalOvercommitTracker>;

static constexpr UInt64 WAIT_TIME = 4'000'000;

template <typename T>
void free_not_continue_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);

    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    std::thread(
        [&]()
        {
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(50);
        }
    ).join();

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, THREADS);
}

TEST(OvercommitTracker, UserFreeNotContinue)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_not_continue_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeNotContinue)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_not_continue_test(global_overcommit_tracker);
}

template <typename T>
void free_continue_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    std::thread(
        [&]()
        {
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
        }
    ).join();

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, 0);
}

TEST(OvercommitTracker, UserFreeContinue)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_continue_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinue)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_test(global_overcommit_tracker);
}

template <typename T>
void free_continue_and_alloc_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    bool stopped_next = false;
    std::thread(
        [&]()
        {
            MemoryTracker failed;
            failed.setOvercommitWaitingTime(WAIT_TIME);
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
            stopped_next = overcommit_tracker.needToStopQuery(&failed, 100) != OvercommitResult::MEMORY_FREED;
        }
    ).join();

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, 0);
    ASSERT_EQ(stopped_next, true);
}

TEST(OvercommitTracker, UserFreeContinueAndAlloc)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_continue_and_alloc_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinueAndAlloc)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_and_alloc_test(global_overcommit_tracker);
}

template <typename T>
void free_continue_and_alloc_2_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    bool stopped_next = false;
    threads.push_back(std::thread(
        [&]()
        {
            MemoryTracker failed;
            failed.setOvercommitWaitingTime(WAIT_TIME);
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
            stopped_next = overcommit_tracker.needToStopQuery(&failed, 100) != OvercommitResult::MEMORY_FREED;
        }
    ));

    threads.push_back(std::thread(
        [&]()
        {
            std::this_thread::sleep_for(2000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(90);
        }
    ));

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, 0);
    ASSERT_EQ(stopped_next, true);
}

TEST(OvercommitTracker, UserFreeContinueAndAlloc2)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_continue_and_alloc_2_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinueAndAlloc2)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_and_alloc_2_test(global_overcommit_tracker);
}

template <typename T>
void free_continue_and_alloc_3_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    bool stopped_next = false;
    threads.push_back(std::thread(
        [&]()
        {
            MemoryTracker failed;
            failed.setOvercommitWaitingTime(WAIT_TIME);
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
            stopped_next = overcommit_tracker.needToStopQuery(&failed, 100) != OvercommitResult::MEMORY_FREED;
        }
    ));

    threads.push_back(std::thread(
        [&]()
        {
            std::this_thread::sleep_for(2000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(100);
        }
    ));

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, 0);
    ASSERT_EQ(stopped_next, false);
}

TEST(OvercommitTracker, UserFreeContinueAndAlloc3)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_continue_and_alloc_3_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinueAndAlloc3)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_and_alloc_3_test(global_overcommit_tracker);
}

template <typename T>
void free_continue_2_test(T & overcommit_tracker)
{
    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    for (auto & tracker : trackers)
        tracker.setOvercommitWaitingTime(WAIT_TIME);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread(
            [&, i]()
            {
                if (overcommit_tracker.needToStopQuery(&trackers[i], 100) != OvercommitResult::MEMORY_FREED)
                    ++need_to_stop;
            }
        ));
    }

    std::thread(
        [&]()
        {
            std::this_thread::sleep_for(1000ms);
            overcommit_tracker.tryContinueQueryExecutionAfterFree(300);
        }
    ).join();

    for (auto & thread : threads)
    {
        thread.join();
    }

    ASSERT_EQ(need_to_stop, 2);
}

TEST(OvercommitTracker, UserFreeContinue2)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    free_continue_2_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinue2)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_2_test(global_overcommit_tracker);
}

template <typename T>
void query_stop_not_continue_test(T & overcommit_tracker)
{
    std::atomic<int> need_to_stop = 0;

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(WAIT_TIME);
    overcommit_tracker.setCandidate(&picked);

    MemoryTracker another;
    another.setOvercommitWaitingTime(WAIT_TIME);
    auto thread = std::thread(
        [&]()
        {
            if (overcommit_tracker.needToStopQuery(&another, 100) != OvercommitResult::MEMORY_FREED)
                ++need_to_stop;
        }
    );
    std::this_thread::sleep_for(1000ms);
    overcommit_tracker.onQueryStop(&picked);
    thread.join();

    ASSERT_EQ(need_to_stop, 1);
}

TEST(OvercommitTracker, UserQueryStopNotContinue)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest user_overcommit_tracker(&process_list, &user_process_list);
    query_stop_not_continue_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalQueryStopNotContinue)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    query_stop_not_continue_test(global_overcommit_tracker);
}

TEST(OvercommitTracker, WaitingTimeKeepsItsSignAndMagnitude)
{
    MemoryTracker tracker;

    /// Zero is reserved for "overcommit waiting is off", and an in-range wait must survive untouched.
    tracker.setOvercommitWaitingTime(0);
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), 0us);
    tracker.setOvercommitWaitingTime(500);
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), 500us);
    tracker.setOvercommitWaitingTime(WAIT_TIME);
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), std::chrono::microseconds(WAIT_TIME));

    /// Two years is representable, so it must not be shortened.
    static constexpr UInt64 two_years = 2ULL * 365 * 24 * 60 * 60 * 1'000'000;
    tracker.setOvercommitWaitingTime(two_years);
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), std::chrono::microseconds(two_years));

    /// Above the signed range an unsigned argument would arrive negative, i.e. already expired.
    tracker.setOvercommitWaitingTime(std::numeric_limits<UInt64>::max());
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), std::chrono::microseconds::max());
    tracker.setOvercommitWaitingTime(1ULL << 63);
    EXPECT_EQ(tracker.getOvercommitWaitingTime(), std::chrono::microseconds::max());
}

/// A wait this long cannot elapse during the test, so a query that stops waiting has lost the
/// requested duration rather than served it.
static void out_of_range_waiting_time_test(UInt64 waiting_time)
{
    ProcessList process_list;
    ProcessListForUser user_process_list(&process_list);
    UserOvercommitTrackerForTest overcommit_tracker(&process_list, &user_process_list);

    MemoryTracker picked;
    picked.setOvercommitWaitingTime(waiting_time);
    overcommit_tracker.setCandidate(&picked);

    MemoryTracker waiting;
    waiting.setOvercommitWaitingTime(waiting_time);
    auto query_selected = overcommit_tracker.getSelectionFuture();
    auto wait_result = std::async(std::launch::async, [&]
    {
        return overcommit_tracker.needToStopQuery(&waiting, 100);
    });

    query_selected.wait();
    EXPECT_EQ(wait_result.wait_for(100ms), std::future_status::timeout);

    overcommit_tracker.onQueryStop(&picked);
    EXPECT_EQ(wait_result.get(), OvercommitResult::NOT_ENOUGH_FREED);
}

TEST(OvercommitTracker, WaitHonoursDurationAboveNanosecondRange)
{
    /// The smallest count whose conversion to nanoseconds does not fit into a signed 64-bit count.
    out_of_range_waiting_time_test(9'223'372'036'854'776);
}

TEST(OvercommitTracker, WaitHonoursDurationAboveSignedRange)
{
    out_of_range_waiting_time_test(std::numeric_limits<UInt64>::max());
}
