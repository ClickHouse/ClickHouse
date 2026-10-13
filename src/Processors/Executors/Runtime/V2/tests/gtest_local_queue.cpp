#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/LocalQueue.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Common/VectorWithMemoryTracking.h>

#include <array>
#include <span>

using namespace DB;
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
    VectorWithMemoryTracking<ExecutingProcessor> records(3);
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
    VectorWithMemoryTracking<ExecutingProcessor> records(5);
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
    VectorWithMemoryTracking<ExecutingProcessor> records(LocalQueue::capacity + 1);
    LocalQueue ring("ring");
    for (UInt32 index = 0; index < LocalQueue::capacity; ++index)
        EXPECT_TRUE(pushOne(ring, records[index]));
    EXPECT_FALSE(pushOne(ring, records.back()));

    std::array<ExecutingProcessor *, LocalQueue::capacity> stolen{};
    EXPECT_EQ(ring.stealFifo(stolen), LocalQueue::capacity / 2);
    EXPECT_TRUE(pushOne(ring, records.back()));
    EXPECT_EQ(ring.size(), LocalQueue::capacity / 2 + 1);
}

WYRD_TEST(LocalQueue, EveryPushedEntryIsTakenExactlyOnce, .runs = 10000, .simulation = { .preempt_probability = 1.0 })
{
    constexpr UInt32 largest_batch = 4;
    const size_t total = Wyrd::randomNumberBetween(8, 64);
    const size_t thieves = Wyrd::randomNumberBetween(3, 4);
    const size_t batch = Wyrd::randomNumberBetween(1, largest_batch);
    const double owner_pop_probability = Wyrd::randomReal(0.1, 0.5);

    VectorWithMemoryTracking<ExecutingProcessor> records(total);
    VectorWithMemoryTracking<Wyrd::Atomic<UInt32>> taken(total);
    auto take = [&](ExecutingProcessor * record) { taken[record - records.data()].fetch_add(1); };

    LocalQueue ring("ring");
    Wyrd::Atomic<bool> owner_done{false};

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

                if (Wyrd::randomEvent(owner_pop_probability))
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
            std::array<ExecutingProcessor *, largest_batch> buffer{};
            std::span<ExecutingProcessor *> stolen(buffer.data(), batch);
            while (!owner_done.load())
            {
                const UInt32 count = ring.stealFifo(stolen);
                for (UInt32 offset = 0; offset < count; ++offset)
                    take(stolen[offset]);
                Wyrd::progress();
            }
        });
    race.run();

    EXPECT_EQ(ring.size(), 0u);
    for (size_t index = 0; index < total; ++index)
        EXPECT_EQ(taken[index].load(), 1u) << "entry " << index;
}
