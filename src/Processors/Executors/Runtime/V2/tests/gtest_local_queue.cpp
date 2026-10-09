#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/LocalQueue.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>

#include <array>
#include <span>
#include <vector>

using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

bool pushOne(LocalQueue & ring, ExecutingProcessor & record)
{
    ExecutingProcessor * entry = &record;
    return ring.pushManyLifo(std::span(&entry, 1)) == 1;
}

}

WYRD_TEST(LocalQueue, OwnerTakesNewestAndThiefTakesOldest)
{
    std::vector<ExecutingProcessor> records(3);
    LocalQueue ring("ring");
    for (auto & record : records)
        EXPECT_TRUE(pushOne(ring, record));

    std::array<ExecutingProcessor *, 1> stolen{};
    EXPECT_EQ(ring.stealFifo(stolen), 1u);
    EXPECT_EQ(stolen[0], records.data());

    EXPECT_EQ(ring.popLifo(), &records[2]);
    EXPECT_EQ(ring.popLifo(), &records[1]);
    EXPECT_EQ(ring.popLifo(), nullptr);
    EXPECT_EQ(ring.stealFifo(stolen), 0u);
    EXPECT_EQ(ring.size(), 0u);
}

WYRD_TEST(LocalQueue, StealTakesHalfRoundedUpWithinSpan)
{
    std::vector<ExecutingProcessor> records(5);
    LocalQueue ring("ring");
    for (auto & record : records)
        EXPECT_TRUE(pushOne(ring, record));

    std::array<ExecutingProcessor *, 8> stolen{};
    EXPECT_EQ(ring.stealFifo(stolen), 3u);
    for (size_t index = 0; index < 3; ++index)
        EXPECT_EQ(stolen[index], &records[index]);

    std::array<ExecutingProcessor *, 1> one{};
    EXPECT_EQ(ring.stealFifo(one), 1u);
    EXPECT_EQ(one[0], &records[3]);

    EXPECT_EQ(ring.popLifo(), &records[4]);
    EXPECT_EQ(ring.popLifo(), nullptr);
    EXPECT_EQ(ring.size(), 0u);
}

WYRD_TEST(LocalQueue, FullRingRefusesPushUntilSteal)
{
    std::vector<ExecutingProcessor> records(LocalQueue::capacity + 1);
    LocalQueue ring("ring");
    for (UInt32 index = 0; index < LocalQueue::capacity; ++index)
        EXPECT_TRUE(pushOne(ring, records[index]));
    EXPECT_FALSE(pushOne(ring, records.back()));

    std::array<ExecutingProcessor *, LocalQueue::capacity> stolen{};
    EXPECT_EQ(ring.stealFifo(stolen), LocalQueue::capacity / 2);
    EXPECT_TRUE(pushOne(ring, records.back()));
    EXPECT_EQ(ring.size(), LocalQueue::capacity / 2 + 1);
}

WYRD_TEST(LocalQueue, EveryPushedEntryIsTakenExactlyOnce, .runs = 10000, .simulation = { .max_syncs_before_preempt = 1 })
{
    constexpr UInt32 total = 64;
    constexpr UInt32 thieves = 3;
    constexpr UInt32 batch = 4;
    constexpr UInt32 owner_pops_one_in = 4;
    constexpr UInt32 min_successful_steals = 4;

    std::vector<ExecutingProcessor> records(total);
    std::vector<Wyrd::Atomic<UInt32>> taken(total);
    auto take = [&](ExecutingProcessor * record) { taken[record - records.data()].fetch_add(1); };

    LocalQueue ring("ring");
    Wyrd::Atomic<bool> owner_done{false};
    Wyrd::Atomic<UInt32> successful_steals{0};

    Wyrd::Race race;
    race.add(
        1,
        [&](size_t)
        {
            Wyrd::LockFreeScope scope;
            for (auto & record : records)
            {
                while (!pushOne(ring, record))
                {
                    if (auto * popped = ring.popLifo())
                        take(popped);
                    Wyrd::progress();
                }
                Wyrd::progress();

                if (Wyrd::randomNumber() % owner_pops_one_in == 0)
                {
                    if (auto * popped = ring.popLifo())
                        take(popped);
                    Wyrd::progress();
                }
            }

            while (auto * popped = ring.popLifo())
            {
                take(popped);
                Wyrd::progress();
            }
            owner_done.store(true);
        });
    race.add(
        thieves,
        [&](size_t)
        {
            Wyrd::LockFreeScope scope;
            std::array<ExecutingProcessor *, batch> stolen{};
            while (!owner_done.load())
            {
                const UInt32 count = ring.stealFifo(stolen);
                for (UInt32 offset = 0; offset < count; ++offset)
                    take(stolen[offset]);
                if (count > 0)
                    successful_steals.fetch_add(1);
                Wyrd::progress();
            }
        });
    race.run();

    EXPECT_EQ(ring.size(), 0u);
    EXPECT_GE(successful_steals.load(), min_successful_steals);
    for (UInt32 index = 0; index < total; ++index)
        EXPECT_EQ(taken[index].load(), 1u) << "entry " << index;
}
