#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include <Common/MemoryTracker.h>
#include <Common/OvercommitTracker.h>
#include <Interpreters/ProcessList.h>

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

static constexpr UInt64 WAIT_TIME = 3'000'000;

template <typename T>
void free_not_continue_test(T & overcommit_tracker)
{
    overcommit_tracker.setMaxWaitTime(WAIT_TIME);

    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread([&, i](){
            if (overcommit_tracker.needToStopQuery(&trackers[i], 100))
                ++need_to_stop;
        }));
    }

    std::thread([&](){
        overcommit_tracker.tryContinueQueryExecutionAfterFree(50);
    }).join();

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
    overcommit_tracker.setMaxWaitTime(WAIT_TIME);

    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread([&, i](){
            if (overcommit_tracker.needToStopQuery(&trackers[i], 100))
                ++need_to_stop;
        }));
    }

    std::thread([&](){
        overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
    }).join();

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
    overcommit_tracker.setMaxWaitTime(WAIT_TIME);

    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread([&, i](){
            if (overcommit_tracker.needToStopQuery(&trackers[i], 100))
                ++need_to_stop;
        }));
    }

    bool stopped_next = false;
    std::thread([&](){
        MemoryTracker failed;
        overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
        stopped_next = overcommit_tracker.needToStopQuery(&failed, 100);
    }).join();

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
    overcommit_tracker.setMaxWaitTime(WAIT_TIME);

    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread([&, i](){
            if (overcommit_tracker.needToStopQuery(&trackers[i], 100))
                ++need_to_stop;
        }));
    }

    bool stopped_next = false;
    threads.push_back(std::thread([&](){
        MemoryTracker failed;
        overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
        stopped_next = overcommit_tracker.needToStopQuery(&failed, 100);
    }));

    overcommit_tracker.tryContinueQueryExecutionAfterFree(90);

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
    overcommit_tracker.setMaxWaitTime(WAIT_TIME);

    static constexpr size_t THREADS = 5;
    std::vector<MemoryTracker> trackers(THREADS);
    std::atomic<int> need_to_stop = 0;
    std::vector<std::thread> threads;
    threads.reserve(THREADS);

    MemoryTracker picked;
    overcommit_tracker.setCandidate(&picked);

    for (size_t i = 0; i < THREADS; ++i)
    {
        threads.push_back(std::thread([&, i](){
            if (overcommit_tracker.needToStopQuery(&trackers[i], 100))
                ++need_to_stop;
        }));
    }

    bool stopped_next = false;
    threads.push_back(std::thread([&](){
        MemoryTracker failed;
        overcommit_tracker.tryContinueQueryExecutionAfterFree(5000);
        stopped_next = overcommit_tracker.needToStopQuery(&failed, 100);
    }));

    overcommit_tracker.tryContinueQueryExecutionAfterFree(100);

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
    free_continue_and_alloc_2_test(user_overcommit_tracker);
}

TEST(OvercommitTracker, GlobalFreeContinueAndAlloc3)
{
    ProcessList process_list;
    GlobalOvercommitTrackerForTest global_overcommit_tracker(&process_list);
    free_continue_and_alloc_2_test(global_overcommit_tracker);
}
