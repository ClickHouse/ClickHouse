#if defined(WYRD_SIMULATION)

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Preemption.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFree.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>

#include <mutex>
#include <set>
#include <stdexcept>

using namespace DB::Runtime::Wyrd;

namespace
{

void incrementInThreeThreads()
{
    Atomic<size_t> counter{0};
    WaitGroup group;
    group.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 20; ++i)
                ++counter;
        });
    group.join();
    EXPECT_EQ(counter.load(), 60u);
}

}

WYRD_TEST(Wyrd, SimulationSameSeedGivesTheSameRun, .runs = 1, .seed = 1)
{
    incrementInThreeThreads();
}

TEST(Wyrd, SimulationDifferentSeedsGiveDifferentSchedules)
{
    std::set<UInt64> digests;
    for (UInt64 seed = 1; seed <= 16; ++seed)
    {
        SimulationParams params;
        params.seed = seed;
        digests.insert(Simulation(params).run(incrementInThreeThreads).digest);
    }

    EXPECT_GT(digests.size(), 1u);
}

TEST(Wyrd, SimulationFindsLockOrderDeadlock)
{
    auto lock_in_opposite_order = []
    {
        Mutex first;
        Mutex second;

        WaitGroup group;
        group.add(
            1,
            [&](size_t)
            {
                std::lock_guard lock_first(first);
                std::lock_guard lock_second(second);
            });
        group.add(
            1,
            [&](size_t)
            {
                std::lock_guard lock_second(second);
                std::lock_guard lock_first(first);
            });
        group.join();
    };

    bool found = false;
    for (UInt64 seed = 1; seed <= 200 && !found; ++seed)
    {
        SimulationParams params;
        params.seed = seed;
        SimulationResult result = Simulation(params).run(lock_in_opposite_order);
        ASSERT_NE(result.status, SimulationResult::Status::Exception) << result.exception_message;
        if (result.status == SimulationResult::Status::Deadlock)
        {
            found = true;
            EXPECT_FALSE(result.last_syscalls.empty());
        }
    }
    EXPECT_TRUE(found);
}

TEST(Wyrd, SimulationReportsAnExceptionFromAThread)
{
    SimulationParams params;
    params.seed = 3;

    SimulationResult result = Simulation(params).run(
        []
        {
            Thread thread([] { throw std::runtime_error("boom"); });
            thread.join();
        });

    EXPECT_EQ(result.status, SimulationResult::Status::Exception);
    EXPECT_NE(result.exception_message.find("boom"), std::string::npos);
}

TEST(Wyrd, SimulationStopsAtTheStepLimit)
{
    SimulationParams params;
    params.step_limit = 50;

    SimulationResult result = Simulation(params).run(
        []
        {
            Atomic<size_t> spinner{0};
            while (true)
                ++spinner;
        });

    EXPECT_EQ(result.status, SimulationResult::Status::StepLimit);
    EXPECT_EQ(result.total_steps, 50u);
}

TEST(Wyrd, SimulationSwitchesAtAPreemptionPoint)
{
    SimulationParams params;
    params.step_limit = 1000;
    params.seed = 43;

    bool done = false;
    SimulationResult result = Simulation(params).run(
        [&]
        {
            Thread setter([&] { done = true; });
            while (!done)
                preemptionPoint();
            setter.join();
        });

    EXPECT_EQ(result.status, SimulationResult::Status::Ok);
    EXPECT_TRUE(done);
    EXPECT_NE(result.last_syscalls.find("preemptionPoint"), std::string::npos);
}

TEST(Wyrd, SimulationHoldsAThreadInsideLockFreeScope)
{
    SimulationParams params;
    params.printed_syscalls = 100'000;
    params.hold_every = 1;
    params.held_threads_limit = 2;

    SimulationResult result = Simulation(params).run(
        []
        {
            Atomic<size_t> counter{0};

            WaitGroup group;
            group.add(
                3,
                [&](size_t)
                {
                    LockFreeScope scope;
                    for (size_t i = 0; i < 50; ++i)
                    {
                        ++counter;
                        progress();
                    }
                });
            group.join();

            EXPECT_EQ(counter.load(), 150u);
        });

    EXPECT_EQ(result.status, SimulationResult::Status::Ok);
    EXPECT_NE(result.last_syscalls.find("held"), std::string::npos);
}

TEST(Wyrd, SimulationFindsBlockingInsideLockFreeScope)
{
    auto block_on_a_lock_free_peer = []
    {
        Atomic<UInt32> busy{0};
        Atomic<size_t> work{0};

        WaitGroup group;
        group.add(
            1,
            [&](size_t)
            {
                LockFreeScope scope;
                busy.store(1);
                for (size_t i = 0; i < 10; ++i)
                {
                    ++work;
                    progress();
                }
                busy.store(0);
                futexWakeAll(busy);
            });
        group.add(
            1,
            [&](size_t)
            {
                LockFreeScope scope;
                UInt32 value = busy.load();
                while (value == 1)
                    futexWaitFetch(busy, value);
            });
        group.join();
    };

    bool found = false;
    for (UInt64 seed = 1; seed <= 200 && !found; ++seed)
    {
        SimulationParams params;
        params.seed = seed;
        params.hold_every = 1;
        found = Simulation(params).run(block_on_a_lock_free_peer).status == SimulationResult::Status::Deadlock;
    }

    EXPECT_TRUE(found);
}

WYRD_TEST(Wyrd, SimulationSpuriousFailureEveryTime, .runs = 1, .simulation = {.spurious_failure_every = 1})
{
    Atomic<int> value{0};
    int expected = 0;
    EXPECT_FALSE(value.compare_exchange_weak(expected, 1));
    EXPECT_EQ(expected, 0);
    EXPECT_TRUE(value.compare_exchange_strong(expected, 1));
}

#endif
