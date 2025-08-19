#include <gtest/gtest.h>

#include <vector>
#include <thread>
#include <pcg_random.hpp>

#include <base/types.h>
#include <base/sleep.h>
#include <Common/ConcurrencyControl.h>
#include <Common/randomSeed.h>

using namespace DB;

struct ConcurrencyControlTest
{
    ConcurrencyControl cc;

    explicit ConcurrencyControlTest(SlotCount limit = UnlimitedSlots)
    {
        cc.setMaxConcurrency(limit);
    }
};

TEST(ConcurrencyControl, Unlimited)
{
    ConcurrencyControlTest t; // unlimited number of slots
    auto slots = t.cc.allocate(0, 100500);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = slots->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 100500);
}

TEST(ConcurrencyControl, Fifo)
{
    ConcurrencyControlTest t(1); // use single slot
    std::vector<SlotAllocationPtr> allocations;
    constexpr int count = 42;
    allocations.reserve(count);
    for (int i = 0; i < count; i++)
        allocations.emplace_back(t.cc.allocate(0, 1));
    for (int i = 0; i < count; i++)
    {
        AcquiredSlotPtr holder;
        for (int j = 0; j < count; j++)
        {
            auto slot = allocations[j]->tryAcquire();
            if (i == j) // check fifo order of allocations
            {
                ASSERT_TRUE(slot);
                holder = std::move(slot);
            }
            else
                ASSERT_TRUE(!slot);
        }
        holder.reset(); // release slot -- leads to the next allocation
    }
}

TEST(ConcurrencyControl, Oversubscription)
{
    ConcurrencyControlTest t(10);
    std::vector<SlotAllocationPtr> allocations;
    allocations.reserve(10);
    for (int i = 0; i < 10; i++)
        allocations.emplace_back(t.cc.allocate(1, 2));
    std::vector<AcquiredSlotPtr> slots;
    // Normal allocation using maximum amount of slots
    for (int i = 0; i < 5; i++)
    {
        auto slot1 = allocations[i]->tryAcquire();
        ASSERT_TRUE(slot1);
        slots.emplace_back(std::move(slot1));
        auto slot2 = allocations[i]->tryAcquire();
        ASSERT_TRUE(slot2);
        slots.emplace_back(std::move(slot2));
        ASSERT_TRUE(!allocations[i]->tryAcquire());
    }
    // Oversubscription: only minimum amount of slots are allocated
    for (int i = 5; i < 10; i++)
    {
        auto slot1 = allocations[i]->tryAcquire();
        ASSERT_TRUE(slot1);
        slots.emplace_back(std::move(slot1));
        ASSERT_TRUE(!allocations[i]->tryAcquire());
    }
}

TEST(ConcurrencyControl, ReleaseUnacquiredSlots)
{
    ConcurrencyControlTest t(10);
    {
        std::vector<SlotAllocationPtr> allocations;
        allocations.reserve(10);
        for (int i = 0; i < 10; i++)
            allocations.emplace_back(t.cc.allocate(1, 2));
        // Do not acquire - just destroy allocations with granted slots
    }
    // Check that slots were actually released
    auto allocation = t.cc.allocate(0, 20);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 10);
}

TEST(ConcurrencyControl, DestroyNotFullyAllocatedAllocation)
{
    ConcurrencyControlTest t(10);
    for (int i = 0; i < 3; i++)
    {
        auto allocation = t.cc.allocate(5, 20);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 10);
    }
}

TEST(ConcurrencyControl, DestroyAllocationBeforeSlots)
{
    ConcurrencyControlTest t(10);
    for (int i = 0; i < 3; i++)
    {
        std::vector<AcquiredSlotPtr> acquired;
        auto allocation = t.cc.allocate(5, 20);
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 10);
        allocation.reset(); // slots are still acquired (they should actually hold allocation)
    }
}

TEST(ConcurrencyControl, GrantReleasedToTheSameAllocation)
{
    ConcurrencyControlTest t(3);
    auto allocation = t.cc.allocate(0, 10);
    std::list<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 3); // 0 1 2
    acquired.clear();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 3); // 3 4 5
    acquired.pop_back();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 3); // 3 4 6
    acquired.pop_front();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 3); // 4 6 7
    acquired.clear();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 2); // 8 9
}

TEST(ConcurrencyControl, FairGranting)
{
    ConcurrencyControlTest t(3);
    auto start_busy_period = t.cc.allocate(3, 3);
    auto a1 = t.cc.allocate(0, 10);
    auto a2 = t.cc.allocate(0, 10);
    auto a3 = t.cc.allocate(0, 10);
    start_busy_period.reset();
    for (int i = 0; i < 10; i++)
    {
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);
        ASSERT_TRUE(!a1->tryAcquire());
        auto s2 = a2->tryAcquire();
        ASSERT_TRUE(s2);
        ASSERT_TRUE(!a2->tryAcquire());
        auto s3 = a3->tryAcquire();
        ASSERT_TRUE(s3);
        ASSERT_TRUE(!a3->tryAcquire());
    }
}

TEST(ConcurrencyControl, SetSlotCount)
{
    ConcurrencyControlTest t(10);
    auto allocation = t.cc.allocate(5, 30);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 10);

    t.cc.setMaxConcurrency(15);
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 15);

    t.cc.setMaxConcurrency(5);
    acquired.clear();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 5);

    // Check that newly added slots are equally distributed over waiting allocations
    std::vector<AcquiredSlotPtr> acquired2;
    auto allocation2 = t.cc.allocate(0, 30);
    ASSERT_TRUE(!allocation->tryAcquire());
    t.cc.setMaxConcurrency(15); // 10 slots added: 5 to the first allocation and 5 to the second one
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    while (auto slot = allocation2->tryAcquire())
        acquired2.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 10);
    ASSERT_TRUE(acquired2.size() == 5);
}

TEST(ConcurrencyControl, MultipleThreads)
{
    constexpr int cfg_total_queries = 1000; // total amount of queries to run
    constexpr int cfg_work_us = 49; // max microseconds per single work
    constexpr int cfg_concurrent_queries = 8; // do not run more than specified number of concurrent queries
    constexpr int cfg_max_threads = 4; // max amount of threads a query is allowed to have
    constexpr int cfg_max_concurrency = 16; // concurrency control limit (must be >3)

    ConcurrencyControlTest t(cfg_max_concurrency);

    auto run_query = [&] (size_t max_threads)
    {
        SlotAllocationPtr slots = t.cc.allocate(1, max_threads);
        std::mutex threads_mutex;
        std::vector<std::thread> threads;
        threads.reserve(max_threads);

        std::function<void()> spawn_threads = [&] ()
        {
            while (auto slot = slots->tryAcquire())
            {
                std::unique_lock lock{threads_mutex};
                threads.emplace_back([&, my_slot = std::move(slot)]
                {
                    pcg64 rng(randomSeed());
                    std::uniform_int_distribution<size_t> distribution(1, cfg_work_us);
                    size_t steps = distribution(rng);
                    for (size_t step = 0; step < steps; ++step)
                    {
                        sleepForMicroseconds(distribution(rng)); // emulate work
                        spawn_threads(); // upscale
                    }
                });
            }
        };

        spawn_threads();

        // graceful shutdown of a query
        for (size_t thread_num = 0; ; thread_num++)
        {
            std::unique_lock lock{threads_mutex};
            if (thread_num >= threads.size())
                break;
            if (threads[thread_num].joinable())
            {
                auto & thread = threads[thread_num];
                lock.unlock(); // to avoid deadlock if thread we are going to join starts spawning threads
                thread.join();
            }
        }
        // NOTE: No races: all concurrent spawn_threads() calls are done from `threads`, but they're already joined.
    };

    pcg64 rng(randomSeed());
    std::uniform_int_distribution<size_t> max_threads_distribution(1, cfg_max_threads);
    std::vector<std::thread> queries;
    std::atomic<int> started = 0; // queries started in total
    std::atomic<int> finished = 0; // queries finished in total
    while (started < cfg_total_queries)
    {
        while (started < finished + cfg_concurrent_queries)
        {
            queries.emplace_back([&, max_threads = max_threads_distribution(rng)]
            {
                run_query(max_threads);
                ++finished;
            });
            ++started;
        }
        sleepForMicroseconds(5); // wait some queries to finish
        t.cc.setMaxConcurrency(cfg_max_concurrency - started % 3); // emulate configuration updates
    }

    for (auto & query : queries)
        query.join();
}
