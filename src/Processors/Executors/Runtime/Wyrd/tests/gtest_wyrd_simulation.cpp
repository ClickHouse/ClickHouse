#if defined(WYRD_SIMULATION)

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>

#include <gtest/gtest.h>

#include <mutex>
#include <set>
#include <stdexcept>

using namespace DB::Runtime::Wyrd;

namespace
{

SimulationResult simulate(UInt64 seed, std::function<void()> body)
{
    SimulationParams params;
    params.seed = seed;
    return Simulation(params, {}).run(std::move(body));
}

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

void lockInOppositeOrder()
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
}

}

TEST(Wyrd, SimulationSameSeedGivesTheSameRun)
{
    SimulationResult first = simulate(1, incrementInThreeThreads);
    SimulationResult second = simulate(1, incrementInThreeThreads);

    EXPECT_EQ(first.status, SimulationResult::Status::Ok);
    EXPECT_EQ(first.digest, second.digest);
    EXPECT_EQ(first.total_steps, second.total_steps);
    EXPECT_GT(first.total_steps, 0u);
}

TEST(Wyrd, SimulationDifferentSeedsGiveDifferentSchedules)
{
    std::set<UInt64> digests;
    for (UInt64 seed = 1; seed <= 16; ++seed)
        digests.insert(simulate(seed, incrementInThreeThreads).digest);

    EXPECT_GT(digests.size(), 1u);
}

TEST(Wyrd, SimulationFindsLockOrderDeadlock)
{
    bool found = false;
    for (UInt64 seed = 1; seed <= 200 && !found; ++seed)
    {
        SimulationResult result = simulate(seed, lockInOppositeOrder);
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
    SimulationResult result = simulate(
        3,
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

    SimulationResult result = Simulation(params, {}).run(
        []
        {
            Atomic<size_t> spinner{0};
            while (true)
                ++spinner;
        });

    EXPECT_EQ(result.status, SimulationResult::Status::StepLimit);
    EXPECT_EQ(result.total_steps, 50u);
}

TEST(Wyrd, SimulationSpuriousFailureEveryTime)
{
    SimulationParams params;
    params.spurious_failure_every = 1;

    SimulationResult result = Simulation(params, {}).run(
        []
        {
            Atomic<int> value{0};
            int expected = 0;
            EXPECT_FALSE(value.compare_exchange_weak(expected, 1));
            EXPECT_EQ(expected, 0);
            EXPECT_TRUE(value.compare_exchange_strong(expected, 1));
        });

    EXPECT_EQ(result.status, SimulationResult::Status::Ok);
}

#endif
