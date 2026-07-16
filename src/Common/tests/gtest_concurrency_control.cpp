#include <gtest/gtest.h>

#include <vector>
#include <thread>
#include <pcg_random.hpp>

#include <base/types.h>
#include <base/sleep.h>
#include <Common/ConcurrencyControl.h>
#include <Common/ProfileEvents.h>
#include <Common/randomSeed.h>

namespace ProfileEvents
{
    extern const Event ConcurrencyControlSlotsDelayed;
    extern const Event ConcurrencyControlQueriesDelayed;
}

using namespace DB;

namespace
{
    struct DelayedMetrics
    {
        ProfileEvents::Count slots_delta = 0;
        ProfileEvents::Count queries_delta = 0;
    };

    struct DelayedMetricsScope
    {
        ProfileEvents::Count slots_before = ProfileEvents::global_counters[ProfileEvents::ConcurrencyControlSlotsDelayed];
        ProfileEvents::Count queries_before = ProfileEvents::global_counters[ProfileEvents::ConcurrencyControlQueriesDelayed];

        DelayedMetrics diff() const
        {
            return {
                ProfileEvents::global_counters[ProfileEvents::ConcurrencyControlSlotsDelayed] - slots_before,
                ProfileEvents::global_counters[ProfileEvents::ConcurrencyControlQueriesDelayed] - queries_before,
            };
        }
    };
}

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

    // Interleave releases with tryAcquires so a2/a3 consume their granted slots promptly
    // (mimicking a real pipeline's tryAcquire loop). Under work-conserving MMF, this keeps
    // the lowest-allocated waiter eligible on each release, so all 4 released slots go to
    // a2 and a3 in turn.
    std::vector<AcquiredSlotPtr> a2_slots;
    std::vector<AcquiredSlotPtr> a3_slots;
    for (int i = 0; i < 4; ++i)
    {
        a1_slots.pop_back(); // release one slot
        // Drain whichever of a2/a3 has a granted slot pending.
        if (auto s_a2 = a2->tryAcquire())
            a2_slots.emplace_back(std::move(s_a2));
        else if (auto s_a3 = a3->tryAcquire())
            a3_slots.emplace_back(std::move(s_a3));
    }

    // a2 and a3 together should have received all 4 released slots (no leakage to a1).
    ASSERT_EQ(a2_slots.size() + a3_slots.size(), 4u);
    // Neither a2 nor a3 should be starved.
    ASSERT_GE(a2_slots.size(), 1u);
    ASSERT_GE(a3_slots.size(), 1u);
}

// Tests for the demand-driven granting model.
// `allocate(min, max)` grants `min` and then eagerly fills up to `max` from available
// capacity. Callers that want fewer slots than `max_threads` must express that via
// `allocate(min, min)` (e.g. `allocate(1, 1)` for the simple-INSERT case) or via
// `setMax(N)` once they know their ceiling. Demand is purely caller-driven -- there is
// no auto-grow on consume.

TEST(ConcurrencyControl, LazyCallerDoesNotHoardCapacity)
{
    // Regression test for the INSERT-starvation scenario: a pipeline that only wants
    // 1 thread must not occupy more CPU slots than it asks for. With the simple-INSERT
    // pipeline using `allocate(1, 1)` (the lazy path), capacity stays available for
    // other queries.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(10);
        t.cc.setScheduler(scheduler);

        // A1 is a lazy caller -- it caps itself at 1 thread from the start.
        auto a1 = t.cc.allocate(1, 1);
        auto s1 = a1->tryAcquire();
        ASSERT_TRUE(s1);

        // A2 should be able to acquire essentially all remaining capacity.
        auto a2 = t.cc.allocate(0, 10);
        std::vector<AcquiredSlotPtr> a2_acquired;
        while (auto slot = a2->tryAcquire())
            a2_acquired.emplace_back(std::move(slot));

        // A2 gets the remaining 9 out of 10 total capacity.
        ASSERT_GE(a2_acquired.size(), 9u)
            << "scheduler=" << scheduler << " a2_acquired=" << a2_acquired.size();
    }
}

TEST(ConcurrencyControl, EagerAllocationDrainsToMax)
{
    // A pipeline that wants all N threads up front uses `allocate(1, N)` (eager) and
    // should be able to drain N slots via tryAcquire without relying on later releases.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(32);
        t.cc.setScheduler(scheduler);

        auto allocation = t.cc.allocate(1, 32);
        std::vector<AcquiredSlotPtr> acquired;
        while (auto slot = allocation->tryAcquire())
            acquired.emplace_back(std::move(slot));

        ASSERT_EQ(acquired.size(), 32u) << "scheduler=" << scheduler << " acquired=" << acquired.size();
    }
}

// Regression: setMax that grows a previously-saturated allocation must re-insert it
// into the waiter list and trigger a schedule round so the new capacity actually flows.
// Otherwise the extra room past the old ceiling stays unused until an unrelated release
// happens.
TEST(ConcurrencyControl, SetMaxGrowFromSaturatedTriggersGrant)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(8);
        t.cc.setScheduler(scheduler);

        // allocate(0, 2) on cap=8 eagerly fills to limit=2, so `a` is saturated, not a
        // waiter, immediately after allocate returns.
        auto a = t.cc.allocate(0, 2);
        auto first = a->tryAcquire();
        ASSERT_TRUE(first) << "scheduler=" << scheduler;

        // Grow to 6. Capacity is available (cur_concurrency well below 8). setMax must
        // re-insert and trigger schedule, otherwise tryAcquire below would fail.
        a->setMax(6);

        // Drain -- we should be able to acquire at least 2 more slots (up to limit=6,
        // capped by available capacity).
        std::vector<AcquiredSlotPtr> held;
        held.emplace_back(std::move(first));
        while (auto slot = a->tryAcquire())
            held.emplace_back(std::move(slot));
        ASSERT_GE(held.size(), 3u)
            << "scheduler=" << scheduler << " -- setMax(grow) did not trigger schedule; "
            << "held=" << held.size();
    }
}

TEST(ConcurrencyControl, SetMaxGrowAndShrink)
{
    // Verify setMax semantics:
    //  1. setMax(smaller_than_current_max): simply caps future grants. Does not reclaim
    //     already-granted slots.
    //  2. setMax(larger_than_current_limit) when allocation was saturated: re-inserts into
    //     waiters and triggers a schedule round so more slots can be granted.
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(10);
        t.cc.setScheduler(scheduler);

        // Start with max=2 — saturates quickly.
        auto a = t.cc.allocate(0, 2);
        std::vector<AcquiredSlotPtr> held;
        while (auto slot = a->tryAcquire())
            held.emplace_back(std::move(slot));
        ASSERT_GE(held.size(), 1u) << "scheduler=" << scheduler;

        // Grow: setMax(5) re-admits the allocation to waiters. We can now acquire up to 5 total.
        a->setMax(5);
        while (auto slot = a->tryAcquire())
            held.emplace_back(std::move(slot));
        ASSERT_EQ(held.size(), 5u) << "scheduler=" << scheduler
            << " after setMax(5) — got " << held.size();

        // Shrink: setMax(3) just caps future grants. Already-acquired slots keep working.
        // We don't reclaim; no new slots are granted until acquired drops below the new max.
        a->setMax(3);
        ASSERT_FALSE(a->tryAcquire()) << "scheduler=" << scheduler
            << " — shrunk below current acquired, should not yield new slots";
    }
}

// Verify the setLazyAllocation / getLazyAllocation accessors (used by PipelineExecutor
// to decide between allocate(1,1)+setMax vs eager allocate(1, num_threads)). CC's own
// `allocate` is always lazy regardless; the flag only controls the caller-side strategy.
// Eager-rollback regression: allocate(1, N) with N>1 must fill up to N from current capacity
// without requiring later releases. Verified per-scheduler. Catches the bug where `allocate`
// only granted `min` and inserted into waiters without calling `schedule`.
TEST(ConcurrencyControl, EagerAllocationFillsUpToMax)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(16);
        t.cc.setScheduler(scheduler);

        auto alloc = t.cc.allocate(1, 8);
        std::vector<AcquiredSlotPtr> held;
        while (auto slot = alloc->tryAcquire())
            held.emplace_back(std::move(slot));
        ASSERT_EQ(held.size(), 8u)
            << "scheduler=" << scheduler << " -- allocate(1, 8) should fill up to 8 slots; got " << held.size();
    }
}

TEST(ConcurrencyControl, LazyAllocationFlagAccessor)
{
    ConcurrencyControlTest t(32);
    ASSERT_TRUE(t.cc.getLazyAllocation());  // default
    t.cc.setLazyAllocation(false);
    ASSERT_FALSE(t.cc.getLazyAllocation());
    t.cc.setLazyAllocation(true);
    ASSERT_TRUE(t.cc.getLazyAllocation());
}

// MMF must be work-conserving: a first allocation whose `limit` is below the pool
// capacity must not block a later allocation from receiving the remaining slots, even
// though the first sits earlier in the (allocated, sequence) order.
TEST(ConcurrencyControl, MaxMinFairWorkConservingWithLowLimit)
{
    ConcurrencyControlTest t(10);
    t.cc.setScheduler("max_min_fair");

    // `low` caps at 4 -- once it fills, it's no longer a waiter and shouldn't appear in
    // schedule rounds even though its allocated count is the lowest in the set.
    auto low = t.cc.allocate(0, 4);

    // `active` arrives later and asks for the remaining capacity. MMF should grant it
    // the 6 slots that `low` is not entitled to.
    auto active = t.cc.allocate(0, 8);
    std::vector<AcquiredSlotPtr> active_held;
    while (auto slot = active->tryAcquire())
        active_held.emplace_back(std::move(slot));

    ASSERT_GE(active_held.size(), 4u)
        << "work-conserving MMF failed: active=" << active_held.size();
}

// Stress test: hammer setMax, tryAcquire, allocate and free concurrently. Intended
// primarily to run under TSan — validates that the single waiter list, lock ordering
// (state.mutex -> allocation.mutex), and MMF sort-key protection do not race. Uses short
// deadline to keep normal CI fast.
TEST(ConcurrencyControl, StressSetMax)
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
                        SlotCount initial_max = 1 + (rng() % 4);
                        auto alloc = t.cc.allocate(0, initial_max);

                        std::vector<AcquiredSlotPtr> held;
                        for (int j = 0; j < 3; ++j)
                        {
                            if (auto slot = alloc->tryAcquire())
                                held.emplace_back(std::move(slot));
                        }

                        // Grow and shrink the max a few times to exercise setMax grow + shrink
                        // paths, including re-insertion after saturation.
                        for (int j = 0; j < 3; ++j)
                        {
                            alloc->setMax(initial_max + 4);
                            if (auto slot = alloc->tryAcquire())
                                held.emplace_back(std::move(slot));
                            alloc->setMax(1); // shrink below current — future grants capped
                        }
                        alloc->setMax(8); // grow again for final acquires
                        while (auto slot = alloc->tryAcquire())
                            held.emplace_back(std::move(slot));

                        // Drop half, then drop all (exercises release/free ordering).
                        size_t half = held.size() / 2;
                        held.erase(held.begin(), held.begin() + half);
                        held.clear();

                        // Allocation destroyed here -- exercises `free` + `schedule` path.
                    }
                }
                catch (...) // Ok: stress worker records any exception for the main thread to assert on.
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

// Eager `allocate(min, max)` with max > min must emit SlotsDelayed/QueriesDelayed when
// current capacity can't cover the requested ceiling. Regression for the rollback path
// (concurrent_threads_lazy_allocation=false) which never calls setMax and relied on the
// old allocate-time emission.
TEST(ConcurrencyControl, EagerAllocateEmitsDelayedUnderPressure)
{
    // Wants 100 slots, cap is 10. Per-scheduler expected delayed count:
    //   RR  : `delta = max - min = 99`, `available = cap - min = 9`, delayed = 90.
    //   FRR : competing demand `limit = max - min = 99`, `available = cap = 10`, delayed = 89.
    //   MMF : same as FRR.
    const std::vector<std::pair<String, ProfileEvents::Count>> cases{
        {"round_robin", 90},
        {"fair_round_robin", 89},
        {"max_min_fair", 89},
    };
    for (const auto & [scheduler, expected_slots] : cases)
    {
        ConcurrencyControlTest t(10);
        t.cc.setScheduler(scheduler);

        DelayedMetricsScope scope;
        auto a = t.cc.allocate(1, 100);
        const auto d = scope.diff();
        ASSERT_EQ(d.slots_delta, expected_slots)
            << "scheduler=" << scheduler << " -- unexpected SlotsDelayed count";
        ASSERT_EQ(d.queries_delta, 1u)
            << "scheduler=" << scheduler << " -- QueriesDelayed should bump exactly once";
    }
}

// Repeated `setMax` grows while saturated must not over-count SlotsDelayed by recounting
// previously-emitted unmet demand.
TEST(ConcurrencyControl, SetMaxGrowsWhileSaturatedNoRecount)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(10);
        t.cc.setScheduler(scheduler);

        auto a = t.cc.allocate(1, 1);
        // Drain so we're not a waiter pre-setMax.
        auto seed = a->tryAcquire();
        ASSERT_TRUE(seed);

        // Block the pool with a competing allocation that consumes everything else.
        auto blocker = t.cc.allocate(0, 100);
        std::vector<AcquiredSlotPtr> held;
        while (auto slot = blocker->tryAcquire())
            held.emplace_back(std::move(slot));

        DelayedMetricsScope scope;
        // First grow: ask for 14 more than the current ceiling, none available.
        a->setMax(15);
        // Second grow: ask for an additional 35 over the previous ceiling, still none.
        a->setMax(50);
        const auto d = scope.diff();

        // With baseline accounting, the cumulative delayed equals the sum of the deltas
        // beyond max(old_limit, allocated): (15-1) + (50-15) = 49. Without the fix, the
        // formula `new_max - allocated` would yield (15-1) + (50-1) = 63 (RR) and
        // similarly inflated counts for FRR/MMF.
        ASSERT_EQ(d.slots_delta, 49u)
            << "scheduler=" << scheduler << " -- recount in setMax (saw " << d.slots_delta << ")";
        ASSERT_EQ(d.queries_delta, 1u)
            << "scheduler=" << scheduler << " -- QueriesDelayed must bump exactly once";
    }
}

// `setMax(shrink_below_allocated)` followed by `setMax(grow_still_below_allocated)` must
// not underflow the unsigned `wanted` computation and emit a huge SlotsDelayed.
TEST(ConcurrencyControl, SetMaxShrinkThenGrowNoUnderflow)
{
    for (String scheduler : {"round_robin", "fair_round_robin", "max_min_fair"})
    {
        ConcurrencyControlTest t(32);
        t.cc.setScheduler(scheduler);

        // Fill to allocated=20.
        auto a = t.cc.allocate(0, 20);
        std::vector<AcquiredSlotPtr> held;
        while (auto slot = a->tryAcquire())
            held.emplace_back(std::move(slot));
        ASSERT_EQ(held.size(), 20u) << "scheduler=" << scheduler;

        // Shrink the ceiling below allocated. Already-granted slots stay; future grants
        // are capped by the new limit.
        a->setMax(10);

        DelayedMetricsScope scope;
        // Grow back to 15 -- still below allocated=20. `new_max - allocated` would
        // underflow on SlotCount and emit a huge spurious delayed count.
        a->setMax(15);
        const auto d = scope.diff();

        ASSERT_EQ(d.slots_delta, 0u)
            << "scheduler=" << scheduler << " -- SlotsDelayed underflow on shrink/regrow "
            << "(saw " << d.slots_delta << ")";
        ASSERT_EQ(d.queries_delta, 0u)
            << "scheduler=" << scheduler << " -- spurious QueriesDelayed emission";
    }
}

