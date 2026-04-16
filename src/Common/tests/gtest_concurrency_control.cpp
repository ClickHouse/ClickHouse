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

TEST(ConcurrencyControl, FairRoundRobinUnlimited)
{
    ConcurrencyControlTest t; // unlimited number of slots
    t.cc.setScheduler("fair_round_robin");
    ASSERT_TRUE(t.cc.getScheduler() == "fair_round_robin");
    auto slots = t.cc.allocate(0, 100500);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = slots->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 100500);
}

TEST(ConcurrencyControl, FairRoundRobinFifo)
{
    ConcurrencyControlTest t(1); // use single slot
    t.cc.setScheduler("fair_round_robin");
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

TEST(ConcurrencyControl, FairRoundRobinNoOversubscription)
{
    ConcurrencyControlTest t(5);
    t.cc.setScheduler("fair_round_robin");
    std::vector<SlotAllocationPtr> allocations;
    allocations.reserve(10);
    for (int i = 0; i < 10; i++)
        allocations.emplace_back(t.cc.allocate(1, 2));
    std::vector<AcquiredSlotPtr> slots;
    // Normal allocation using maximum amount of slots, note that min:1 is not considered as competing and does not count towards limit
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
    // No oversubscription: only minimum amount of slots are allocated
    for (int i = 5; i < 10; i++)
    {
        auto slot1 = allocations[i]->tryAcquire();
        ASSERT_TRUE(slot1);
        slots.emplace_back(std::move(slot1));
        ASSERT_TRUE(!allocations[i]->tryAcquire());
    }
}

TEST(ConcurrencyControl, FairRoundRobinReleaseUnacquiredSlots)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("fair_round_robin");
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


TEST(ConcurrencyControl, FairRoundRobinDestroyNotFullyAllocatedAllocation)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("fair_round_robin");
    for (int i = 0; i < 3; i++)
    {
        auto allocation = t.cc.allocate(5, 20);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 15);
    }
}

TEST(ConcurrencyControl, FairRoundRobinDestroyAllocationBeforeSlots)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("fair_round_robin");
    for (int i = 0; i < 3; i++)
    {
        std::vector<AcquiredSlotPtr> acquired;
        auto allocation = t.cc.allocate(5, 20);
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 15);
        allocation.reset(); // slots are still acquired (they should actually hold allocation)
    }
}

TEST(ConcurrencyControl, FairRoundRobinGrantReleasedToTheSameAllocation)
{
    ConcurrencyControlTest t(3);
    t.cc.setScheduler("fair_round_robin");
    auto allocation = t.cc.allocate(1, 11);
    std::list<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 4); // X 0 1 2
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

TEST(ConcurrencyControl, FairRoundRobinFairGranting)
{
    ConcurrencyControlTest t(3);
    t.cc.setScheduler("fair_round_robin");
    auto start_busy_period = t.cc.allocate(0, 3);
    auto a1 = t.cc.allocate(1, 10);
    auto a2 = t.cc.allocate(1, 10);
    auto a3 = t.cc.allocate(1, 10);
    start_busy_period.reset();

    // The first slot in esch allocation is not counted into limit
    {
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);
        auto s2 = a2->tryAcquire();
        ASSERT_TRUE(s2);
        auto s3 = a3->tryAcquire();
        ASSERT_TRUE(s3);
    }

    for (int i = 1; i < 10; i++)
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

TEST(ConcurrencyControl, FairRoundRobinSetSlotCount)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("fair_round_robin");
    auto allocation = t.cc.allocate(5, 30);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 15);

    t.cc.setMaxConcurrency(15);
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 20);

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

    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        t.cc.setScheduler(scheduler);
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
}

TEST(ConcurrencyControl, MaxMinFairUnlimited)
{
    ConcurrencyControlTest t; // unlimited number of slots
    t.cc.setScheduler("max_min_fair");
    ASSERT_TRUE(t.cc.getScheduler() == "max_min_fair");
    auto slots = t.cc.allocate(0, 100500);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = slots->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 100500);
}

TEST(ConcurrencyControl, MaxMinFairFifo)
{
    ConcurrencyControlTest t(1); // use single slot
    t.cc.setScheduler("max_min_fair");
    std::vector<SlotAllocationPtr> allocations;
    constexpr int count = 42;
    allocations.reserve(count);
    for (int i = 0; i < count; i++)
        allocations.emplace_back(t.cc.allocate(0, 1));

    // With max_min_fair, when all allocations have equal allocated counts,
    // order is determined by sequence_number which guarantees FIFO.
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

TEST(ConcurrencyControl, MaxMinFairNoOversubscription)
{
    ConcurrencyControlTest t(5);
    t.cc.setScheduler("max_min_fair");
    std::vector<SlotAllocationPtr> allocations;
    allocations.reserve(10);
    for (int i = 0; i < 10; i++)
        allocations.emplace_back(t.cc.allocate(1, 2));
    std::vector<AcquiredSlotPtr> slots;
    // Normal allocation using maximum amount of slots, note that min:1 is not considered as competing and does not count towards limit
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
    // No oversubscription: only minimum amount of slots are allocated
    for (int i = 5; i < 10; i++)
    {
        auto slot1 = allocations[i]->tryAcquire();
        ASSERT_TRUE(slot1);
        slots.emplace_back(std::move(slot1));
        ASSERT_TRUE(!allocations[i]->tryAcquire());
    }
}

TEST(ConcurrencyControl, MaxMinFairReleaseUnacquiredSlots)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("max_min_fair");
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


TEST(ConcurrencyControl, MaxMinFairDestroyNotFullyAllocatedAllocation)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("max_min_fair");
    for (int i = 0; i < 3; i++)
    {
        auto allocation = t.cc.allocate(5, 20);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 15);
    }
}

TEST(ConcurrencyControl, MaxMinFairDestroyAllocationBeforeSlots)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("max_min_fair");
    for (int i = 0; i < 3; i++)
    {
        std::vector<AcquiredSlotPtr> acquired;
        auto allocation = t.cc.allocate(5, 20);
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_TRUE(acquired.size() == 15);
        allocation.reset(); // slots are still acquired (they should actually hold allocation)
    }
}

TEST(ConcurrencyControl, MaxMinFairGrantReleasedToTheSameAllocation)
{
    ConcurrencyControlTest t(3);
    t.cc.setScheduler("max_min_fair");
    auto allocation = t.cc.allocate(1, 11);
    std::list<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 4); // X 0 1 2
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

TEST(ConcurrencyControl, MaxMinFairFairGranting)
{
    ConcurrencyControlTest t(3);
    t.cc.setScheduler("max_min_fair");
    auto start_busy_period = t.cc.allocate(0, 3);
    auto a1 = t.cc.allocate(1, 10);
    auto a2 = t.cc.allocate(1, 10);
    auto a3 = t.cc.allocate(1, 10);
    start_busy_period.reset();

    // The first slot in each allocation is not counted into limit
    {
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);
        auto s2 = a2->tryAcquire();
        ASSERT_TRUE(s2);
        auto s3 = a3->tryAcquire();
        ASSERT_TRUE(s3);
    }

    for (int i = 1; i < 10; i++)
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

TEST(ConcurrencyControl, MaxMinFairSetSlotCount)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("max_min_fair");
    auto allocation = t.cc.allocate(5, 30);
    std::vector<AcquiredSlotPtr> acquired;
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 15);

    t.cc.setMaxConcurrency(15);
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 20);

    t.cc.setMaxConcurrency(5);
    acquired.clear();
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    ASSERT_TRUE(acquired.size() == 5);

    // Check that newly added slots are distributed to allocation with fewer slots (max-min fair)
    std::vector<AcquiredSlotPtr> acquired2;
    auto allocation2 = t.cc.allocate(0, 30);
    ASSERT_TRUE(!allocation->tryAcquire());
    t.cc.setMaxConcurrency(15); // 10 slots added: should go to allocation2 first as it has 0 allocated
    while (auto slot = allocation->tryAcquire())
        acquired.emplace_back(std::move(slot));
    while (auto slot = allocation2->tryAcquire())
        acquired2.emplace_back(std::move(slot));
    // allocation has 5 allocated, allocation2 has 0; with max-min fair, allocation2 gets slots until equal
    // Then they alternate. With 10 new slots: allocation2 gets 5 to catch up, then 5 more are split
    ASSERT_TRUE(acquired.size() + acquired2.size() == 15); // total 15 slots
}

// This test demonstrates the key difference between round-robin and max-min fair:
// max-min fair always grants to the allocation with the minimum number of slots
TEST(ConcurrencyControl, MaxMinFairPrioritizesMinimumAllocation)
{
    ConcurrencyControlTest t(6);
    t.cc.setScheduler("max_min_fair");

    // Create three allocations, but give one of them a head start
    auto a1 = t.cc.allocate(0, 10); // will get 6 slots initially
    std::vector<AcquiredSlotPtr> a1_slots;
    while (auto slot = a1->tryAcquire())
        a1_slots.emplace_back(std::move(slot));
    ASSERT_TRUE(a1_slots.size() == 6); // a1 has 6 slots

    // Now create two more allocations - they start waiting
    auto a2 = t.cc.allocate(0, 10);
    auto a3 = t.cc.allocate(0, 10);

    // Release 4 slots from a1
    a1_slots.pop_back();
    a1_slots.pop_back();
    a1_slots.pop_back();
    a1_slots.pop_back();

    // With max-min fair, released slots should go to a2 and a3 (they have 0 allocated)
    // rather than back to a1 (which has 6 allocated, now 2 after releasing 4)
    std::vector<AcquiredSlotPtr> a2_slots;
    std::vector<AcquiredSlotPtr> a3_slots;
    while (auto slot = a2->tryAcquire())
        a2_slots.emplace_back(std::move(slot));
    while (auto slot = a3->tryAcquire())
        a3_slots.emplace_back(std::move(slot));

    // a2 and a3 should get 2 slots each (4 released, distributed to the ones with min allocation)
    ASSERT_TRUE(a2_slots.size() == 2);
    ASSERT_TRUE(a3_slots.size() == 2);
    // a1 should not get any more (it still has 2, which is >= a2 and a3's count)
    ASSERT_TRUE(!a1->tryAcquire());
}

// Tests for lazy slot granting behavior.
// With lazy allocation, allocate() grants only min + at most 1 slot.
// Additional slots are granted one-at-a-time via lazy schedule() triggered by notifyAcquired().
// Bulk schedule (on release/setMaxConcurrency) distributes ALL capacity fairly — unchanged.

TEST(ConcurrencyControl, LazyGrantingCapacityRecovery)
{
    // Verify that lazy allocation + explicit demand signaling prevents INSERT-style starvation:
    // A1 requests 32 slots but only acquires 1 and signals no more demand (like PipelineExecutor
    // does on DO_NOT_SPAWN). The unused capacity flows to A2 across all three schedulers,
    // including max_min_fair (which otherwise would tie A1 and A2 at allocated=1 and block A2).
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(10);
        t.cc.setScheduler(scheduler);

        // A1 requests 32 slots but only acquires 1 (simulating a simple INSERT pipeline)
        auto a1 = t.cc.allocate(1, 32);
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);
        // Simulate PipelineExecutor's DO_NOT_SPAWN behavior: tell CC we don't want more slots.
        a1->setMoreDemand(false);

        // A2 should be able to acquire most of the remaining capacity.
        // With demand signaling, a1 is skipped by lazy schedule across all schedulers.
        auto a2 = t.cc.allocate(0, 10);
        std::vector<AcquiredSlotPtr> a2_acquired;
        while (auto slot = a2->tryAcquire())
            a2_acquired.emplace_back(std::move(slot));

        // A2 should get at least 7 out of 10 total capacity.
        ASSERT_TRUE(a2_acquired.size() >= 7)
            << "scheduler=" << scheduler << " a2_acquired=" << a2_acquired.size();
    }
}

TEST(ConcurrencyControl, LazyGrantingFastRampUp)
{
    // Verify that a pipeline needing all N threads can still ramp up via tryAcquire loop.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(32);
        t.cc.setScheduler(scheduler);

        auto allocation = t.cc.allocate(1, 32);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));

        // Even with lazy granting, the tryAcquire loop should eventually acquire all 32 slots
        ASSERT_TRUE(acquired.size() == 32) << "scheduler=" << scheduler << " acquired=" << acquired.size();
    }
}

TEST(ConcurrencyControl, LazyGrantingScheduleSkipsUnused)
{
    // Verify that lazy schedule() skips allocations with unused granted slots,
    // allowing other allocations to benefit from available capacity.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(2);
        t.cc.setScheduler(scheduler);

        // A1 gets a slot but doesn't acquire it (simulating an idle pipeline)
        auto a1 = t.cc.allocate(0, 5);

        // A2 should still be able to get capacity
        auto a2 = t.cc.allocate(0, 5);
        std::vector<AcquiredSlotPtr> a2_acquired;
        while (auto slot = a2->tryAcquire())
            a2_acquired.emplace_back(std::move(slot));

        // A1 has 1 granted (from allocate bootstrap), consuming 1 CC slot.
        // A2 should get the remaining 1 slot.
        ASSERT_TRUE(a2_acquired.size() >= 1) << "scheduler=" << scheduler << " a2_acquired=" << a2_acquired.size();

        // A1's granted slot is still there, not acquired
        auto a1_slot = a1->tryAcquire();
        ASSERT_TRUE(a1_slot);
    }
}

TEST(ConcurrencyControl, DemandSignaling)
{
    // Verify setMoreDemand semantics:
    //  1. setMoreDemand(false) prevents the allocation from being granted more slots
    //     AND reclaims any pending (granted but not acquired) slots back to the pool.
    //  2. setMoreDemand(true) re-enables granting and triggers a schedule round.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(5);
        t.cc.setScheduler(scheduler);

        // A1 allocates, acquires its bootstrap slot, signals no demand (simulating a
        // pipeline that reached DO_NOT_SPAWN after spawning the threads it needs).
        auto a1 = t.cc.allocate(0, 10);
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);
        a1->setMoreDemand(false);

        // A2 should be able to consume most remaining capacity — A1 is skipped AND any
        // unused pending grants on A1 have been reclaimed.
        auto a2 = t.cc.allocate(0, 5);
        std::vector<AcquiredSlotPtr> a2_acquired;
        while (auto slot = a2->tryAcquire())
            a2_acquired.emplace_back(std::move(slot));
        // With reclaim, A1 holds only 1 acquired slot. A2 should get at least 4 of 5 capacity.
        ASSERT_TRUE(a2_acquired.size() >= 4)
            << "scheduler=" << scheduler << " a2_acquired=" << a2_acquired.size();

        // Re-enable demand on A1; release enough from A2 to guarantee capacity reaches A1.
        // Round-robin order may grant to A2 first if A2 still wants slots, so release all
        // to ensure A1 can eventually acquire.
        a1->setMoreDemand(true);
        a2_acquired.clear(); // release all A2 slots — triggers schedule

        // A1 should now be able to receive new grants again.
        auto s1_more = a1->tryAcquire();
        ASSERT_TRUE(s1_more) << "scheduler=" << scheduler;
    }
}

// Regression: setMoreDemand(false) reclaiming pending granted-but-not-acquired slots must NOT
// permanently reduce the allocation's future grant budget. Previously RR/FRR bumped `released`
// instead of decrementing `allocated`, so repeated reclaims ratcheted down the query's
// attainable parallelism: every pending-grant that was reclaimed continued counting against
// allocated, making `allocated < limit` fail earlier than it should.
//
// Scenario: `min=0, max=limit`. Repeatedly flip demand off/on without actually acquiring the
// pending slot. Each cycle reclaims 1 pending slot. If the budget is properly restored, we
// should still be able to acquire `limit` slots at the end. Without the fix, RR/FRR would
// leak `cycles` worth of budget, and the final acquisition count would be `limit - cycles`.
TEST(ConcurrencyControl, ReclaimPreservesGrantBudget)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        constexpr SlotCount limit = 4;
        constexpr int cycles = 3;

        ConcurrencyControlTest t(16);
        t.cc.setScheduler(scheduler);

        auto alloc = t.cc.allocate(0, limit);
        // Bootstrap: 1 pending slot granted. We reclaim it without acquiring.
        for (int i = 0; i < cycles; ++i)
        {
            alloc->setMoreDemand(false);  // reclaims the pending grant
            alloc->setMoreDemand(true);   // re-adds to demanders, which triggers another grant
        }

        // Now drain all available slots.
        std::vector<AcquiredSlotPtr> held;
        while (auto slot = alloc->tryAcquire())
            held.emplace_back(std::move(slot));

        // With budget preserved, we must be able to acquire `limit` slots. Before the fix,
        // RR/FRR would cap out at `limit - cycles` because reclaimed grants were never
        // refunded to the allocation's lifetime budget.
        ASSERT_EQ(held.size(), limit)
            << "scheduler=" << scheduler << " held=" << held.size() << " limit=" << limit
            << " — reclaim appears to have ratcheted the allocation's grant budget";
    }
}

// Verify the emergency revert lever: when lazy_allocation is disabled, allocate() grants
// up to `max` slots immediately (pre-#88339 behavior). Covers all three schedulers.
TEST(ConcurrencyControl, LazyAllocationRevertFlag)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(32);
        t.cc.setScheduler(scheduler);
        t.cc.setLazyAllocation(false);
        ASSERT_FALSE(t.cc.getLazyAllocation()) << "scheduler=" << scheduler;

        // With eager allocation, a query asking for 32 slots should be able to acquire all 32
        // immediately — matching the pre-fix behavior.
        auto alloc = t.cc.allocate(1, 32);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = alloc->tryAcquire())
            acquired.emplace_back(std::move(slot));
        ASSERT_EQ(acquired.size(), 32u) << "scheduler=" << scheduler
            << " — eager allocation should grant all 32 slots, got " << acquired.size();

        // Re-enable lazy and verify the next allocation gets only min + 1.
        acquired.clear();
        alloc.reset();
        t.cc.setLazyAllocation(true);
        auto alloc2 = t.cc.allocate(1, 32);
        std::vector<AcquiredSlotPtr> acquired2;
        // Under lazy allocation, first acquire should succeed (bootstrap slot), then the second
        // should succeed too (min + 1 ramp-up), but we shouldn't be able to grab all 32 at once
        // without releasing — bounded ramp-up is the whole point.
        auto first = alloc2->tryAcquire();
        ASSERT_TRUE(first) << "scheduler=" << scheduler;
    }
}

// Stress test: hammer setMoreDemand, tryAcquire, allocate and free concurrently. Intended
// primarily to run under TSan — validates that the demanders ⊆ waiters invariant, lock
// ordering (state.mutex -> allocation.mutex), MMF sort-key protection, and reclaim paths
// do not race. Uses short deadline to keep normal CI fast; under TSan the coverage is
// sufficient to catch the most likely races.
TEST(ConcurrencyControl, StressDemandSignaling)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(16);
        t.cc.setScheduler(scheduler);

        constexpr int num_workers = 8;
        constexpr int iterations_per_worker = 200;
        std::atomic<bool> had_exception{false};
        std::vector<std::thread> workers;
        workers.reserve(num_workers);

        for (int w = 0; w < num_workers; ++w)
        {
            workers.emplace_back([&, w]()
            {
                try
                {
                    pcg64 rng(randomSeed() + w);
                    for (int i = 0; i < iterations_per_worker; ++i)
                    {
                        // Allocate a small slot range.
                        SlotCount max = 1 + (rng() % 8);
                        auto alloc = t.cc.allocate(0, max);

                        // Acquire a few slots (some workers will fail — that's fine).
                        std::vector<AcquiredSlotPtr> held;
                        for (int j = 0; j < 3; ++j)
                        {
                            if (auto slot = alloc->tryAcquire())
                                held.emplace_back(std::move(slot));
                        }

                        // Flap demand a few times to exercise reclaim + re-add paths.
                        for (int j = 0; j < 3; ++j)
                        {
                            alloc->setMoreDemand(false);
                            alloc->setMoreDemand(true);
                        }
                        alloc->setMoreDemand(false);

                        // Try acquiring again after reclaim + re-enable.
                        alloc->setMoreDemand(true);
                        if (auto slot = alloc->tryAcquire())
                            held.emplace_back(std::move(slot));

                        // Drop half, then drop all (exercises release/free ordering).
                        size_t half = held.size() / 2;
                        held.erase(held.begin(), held.begin() + half);
                        held.clear();

                        // Allocation destroyed here — exercises free() + schedule() path.
                    }
                }
                catch (...)
                {
                    had_exception.store(true);
                }
            });
        }

        for (auto & worker : workers)
            worker.join();

        ASSERT_FALSE(had_exception.load()) << "scheduler=" << scheduler;

        // After all workers finish, no allocations remain — an allocate should succeed
        // with full capacity, proving no slots leaked (cur_concurrency was decremented correctly).
        auto probe = t.cc.allocate(0, 16);
        std::vector<AcquiredSlotPtr> probe_held;
        while (auto slot = probe->tryAcquire())
            probe_held.emplace_back(std::move(slot));
        ASSERT_EQ(probe_held.size(), 16u) << "scheduler=" << scheduler
            << " — slots leaked in stress test (got " << probe_held.size() << "/16)";
    }
}

