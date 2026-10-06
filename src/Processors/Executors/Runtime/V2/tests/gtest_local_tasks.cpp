#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorState.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
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
    EXPECT_EQ(ring.grab(stolen), 1u);
    EXPECT_EQ(&stolen[0].state(), states.data());
    EXPECT_EQ(stolen[0].step(), Task::Step::Work);

    EXPECT_EQ(popped(ring), &states[2]);
    EXPECT_EQ(popped(ring), &states[1]);
    EXPECT_EQ(popped(ring), nullptr);
    EXPECT_EQ(ring.grab(stolen), 0u);
    EXPECT_EQ(ring.size(), 0u);
}

WYRD_TEST(LocalTasks, EveryPushedTaskIsTakenExactlyOnce)
{
    constexpr UInt32 total = 300;
    constexpr UInt32 thieves = 3;
    constexpr UInt32 batch = 4;

    std::vector<ProcessorState> states(total);
    std::vector<Wyrd::Atomic<UInt32>> taken(total);
    auto take = [&](Task task) { taken[&task.state() - states.data()].fetch_add(1); };

    LocalTasks ring;
    Wyrd::Atomic<bool> owner_done{false};

    Wyrd::Race race;
    race.add(
        1,
        [&](size_t)
        {
            for (auto & state : states)
            {
                while (!ring.push(taskFor(state)))
                    if (auto task = ring.pop())
                        take(*task);

                if (Wyrd::randomNumber() % 2 == 0)
                    if (auto task = ring.pop())
                        take(*task);
            }

            while (auto task = ring.pop())
                take(*task);
            owner_done.store(true);
        });
    race.add(
        thieves,
        [&](size_t)
        {
            std::array<Task, batch> grabbed{};
            while (true)
            {
                const UInt32 count = ring.grab(grabbed);
                for (UInt32 offset = 0; offset < count; ++offset)
                    take(grabbed[offset]);

                if (count == 0 && owner_done.load())
                    return;
            }
        });
    race.run();

    EXPECT_EQ(ring.size(), 0u);
    for (UInt32 index = 0; index < total; ++index)
        EXPECT_EQ(taken[index].load(), 1u) << "task " << index;
}
