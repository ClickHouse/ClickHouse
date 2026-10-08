#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorState.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>

#include <array>
#include <vector>

using namespace DB::Runtime;
using namespace DB::Runtime::V2;

namespace
{

Task taskFor(ProcessorState & state)
{
    return Task::create(state, Task::Step::Work);
}

const ProcessorState * popped(LocalTasks & ring)
{
    auto task = ring.pop();
    return task ? &task->state() : nullptr;
}

}

WYRD_TEST(LocalTasks, OwnerTakesNewestAndThiefTakesOldest)
{
    std::vector<ProcessorState> states(3);
    LocalTasks ring;
    for (auto & state : states)
        EXPECT_TRUE(ring.push(taskFor(state)));

    std::array<Task, 1> stolen{};
    EXPECT_EQ(ring.steal(stolen), 1u);
    EXPECT_EQ(&stolen[0].state(), states.data());
    EXPECT_EQ(stolen[0].step(), Task::Step::Work);

    EXPECT_EQ(popped(ring), &states[2]);
    EXPECT_EQ(popped(ring), &states[1]);
    EXPECT_EQ(popped(ring), nullptr);
    EXPECT_EQ(ring.steal(stolen), 0u);
    EXPECT_EQ(ring.size(), 0u);
}

WYRD_TEST(LocalTasks, StealTakesHalfRoundedUpWithinSpan)
{
    std::vector<ProcessorState> states(5);
    LocalTasks ring;
    for (auto & state : states)
        EXPECT_TRUE(ring.push(taskFor(state)));

    std::array<Task, 8> stolen{};
    EXPECT_EQ(ring.steal(stolen), 3u);
    for (size_t index = 0; index < 3; ++index)
        EXPECT_EQ(&stolen[index].state(), &states[index]);

    std::array<Task, 1> one{};
    EXPECT_EQ(ring.steal(one), 1u);
    EXPECT_EQ(&one[0].state(), &states[3]);

    EXPECT_EQ(popped(ring), &states[4]);
    EXPECT_EQ(popped(ring), nullptr);
    EXPECT_EQ(ring.size(), 0u);
}

WYRD_TEST(LocalTasks, FullRingRefusesPushUntilSteal)
{
    std::vector<ProcessorState> states(LocalTasks::capacity + 1);
    LocalTasks ring;
    for (UInt32 index = 0; index < LocalTasks::capacity; ++index)
        EXPECT_TRUE(ring.push(taskFor(states[index])));
    EXPECT_FALSE(ring.push(taskFor(states.back())));

    std::array<Task, LocalTasks::capacity> stolen{};
    EXPECT_EQ(ring.steal(stolen), LocalTasks::capacity / 2);
    EXPECT_TRUE(ring.push(taskFor(states.back())));
    EXPECT_EQ(ring.size(), LocalTasks::capacity / 2 + 1);
}

WYRD_TEST(LocalTasks, EveryPushedTaskIsTakenExactlyOnce, .runs = 10000, .simulation = { .max_syncs_before_preempt = 1 })
{
    constexpr UInt32 total = 64;
    constexpr UInt32 thieves = 3;
    constexpr UInt32 batch = 4;
    constexpr UInt32 owner_pops_one_in = 4;
    constexpr UInt32 min_successful_steals = 4;

    std::vector<ProcessorState> states(total);
    std::vector<Wyrd::Atomic<UInt32>> taken(total);
    auto take = [&](Task task) { taken[&task.state() - states.data()].fetch_add(1); };

    LocalTasks ring;
    Wyrd::Atomic<bool> owner_done{false};
    Wyrd::Atomic<UInt32> successful_steals{0};

    Wyrd::Race race;
    race.add(
        1,
        [&](size_t)
        {
            Wyrd::LockFreeScope scope;
            for (auto & state : states)
            {
                while (!ring.push(taskFor(state)))
                {
                    if (auto task = ring.pop())
                        take(*task);
                    Wyrd::progress();
                }
                Wyrd::progress();

                if (Wyrd::randomNumber() % owner_pops_one_in == 0)
                {
                    if (auto task = ring.pop())
                        take(*task);
                    Wyrd::progress();
                }
            }

            while (auto task = ring.pop())
            {
                take(*task);
                Wyrd::progress();
            }
            owner_done.store(true);
        });
    race.add(
        thieves,
        [&](size_t)
        {
            Wyrd::LockFreeScope scope;
            std::array<Task, batch> stolen{};
            while (!owner_done.load())
            {
                const UInt32 count = ring.steal(stolen);
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
        EXPECT_EQ(taken[index].load(), 1u) << "task " << index;
}
