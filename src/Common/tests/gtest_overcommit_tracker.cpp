#include <gtest/gtest.h>
#include <chrono>
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

protected:
    void pickQueryToExcludeImpl() override
    {
        BaseTracker::picked_tracker = tracker;
    }

    MemoryTracker * tracker;
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
