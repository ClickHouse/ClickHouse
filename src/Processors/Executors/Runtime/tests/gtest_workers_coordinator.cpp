#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

struct Fixture
{
    std::vector<ProcessorState> states;
    Poller poller;
    TaskScheduler scheduler;
    WorkersCoordinator coordinator;

    explicit Fixture(size_t workers, size_t states_count = 16)
        : states(states_count)
        , scheduler(poller, workers)
        , coordinator(scheduler, poller)
    {
    }

    Task task(size_t i) { return Task{.state = &states[i], .kind = Task::Kind::Prepare}; }

    /// The worker loop of pickTask: pop, or wait; nullopt once stopped.
    std::optional<Task> pickTask(size_t worker_id)
    {
        while (true)
        {
            if (auto popped = scheduler.tryPop(worker_id))
                return popped;
            if (!coordinator.wait(worker_id))
                return std::nullopt;
        }
    }
};

}

TEST(WorkersCoordinator, WaitReturnsAtOnceWhenTasksAreVisible)
{
    Fixture f(1);
    f.coordinator.enter(0);

    f.scheduler.push(f.task(0));
    EXPECT_TRUE(f.coordinator.wait(0));
    EXPECT_FALSE(f.coordinator.stopped());
}

TEST(WorkersCoordinator, LastIdleWorkerDetectsTheFinish)
{
    Fixture f(2);
    f.coordinator.enter(0);
    f.coordinator.enter(1);

    std::atomic_bool first_returned = false;
    bool first_result = true;
    std::thread first([&]
    {
        first_result = f.coordinator.wait(0);
        first_returned = true;
    });

    while (f.coordinator.idle() == 0)
        std::this_thread::yield();
    EXPECT_FALSE(first_returned);
    EXPECT_FALSE(f.coordinator.stopped());

    EXPECT_FALSE(f.coordinator.wait(1));
    first.join();

    EXPECT_FALSE(first_result);
    EXPECT_TRUE(f.coordinator.stopped());
}

TEST(WorkersCoordinator, WakeOneWakesAnIdleWorkerThatThenSteals)
{
    Fixture f(2);
    f.coordinator.enter(0);
    f.coordinator.enter(1);

    std::optional<Task> picked;
    std::thread idle_worker([&] { picked = f.pickTask(0); });

    while (f.coordinator.idle() == 0)
        std::this_thread::yield();

    f.scheduler.push(f.task(3), 1);
    f.coordinator.wakeOne();
    idle_worker.join();

    ASSERT_TRUE(picked);
    EXPECT_EQ(&f.states[3], picked->state);
    EXPECT_FALSE(f.coordinator.stopped());
}

TEST(WorkersCoordinator, LeaveHandsTasksOverAndTheLastOneStops)
{
    Fixture f(2);
    f.coordinator.enter(0);
    f.coordinator.enter(1);
    f.scheduler.push(f.task(5), 0);

    std::vector<Task> picked;
    std::thread other([&]
    {
        while (auto task = f.pickTask(1))
            picked.push_back(*task);
    });

    while (f.coordinator.idle() == 0)
        std::this_thread::yield();

    f.coordinator.leave(0);
    other.join();

    ASSERT_EQ(1u, picked.size());
    EXPECT_EQ(&f.states[5], picked.front().state);
    EXPECT_TRUE(f.coordinator.stopped());
    EXPECT_EQ(1u, f.coordinator.registered());
}

TEST(WorkersCoordinator, NoWakeUpIsLostUnderConcurrentPushAndWait)
{
    constexpr size_t tasks_count = 20000;

    Fixture f(2, tasks_count);
    f.coordinator.enter(0);
    f.coordinator.enter(1);

    std::atomic<size_t> consumed = 0;
    std::thread consumer([&]
    {
        while (f.pickTask(0))
            ++consumed;
    });

    for (size_t i = 0; i < tasks_count; ++i)
    {
        f.scheduler.push(f.task(i), 1);
        f.coordinator.wakeOne();
    }
    f.coordinator.leave(1);

    consumer.join();
    EXPECT_EQ(tasks_count, consumed.load());
    EXPECT_TRUE(f.coordinator.stopped());
}
