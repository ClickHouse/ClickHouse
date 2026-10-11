#if defined(WYRD_SIMULATION)

#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Preemption.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Simulation.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/LockFreeScope.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Test.h>
#include <Processors/Executors/Runtime/Wyrd/Testing/Race.h>
#include <Common/SetWithMemoryTracking.h>

#include <mutex>
#include <stdexcept>

using namespace DB::Runtime::Wyrd;

namespace
{

void incrementInThreeThreads()
{
    Atomic<size_t> counter{0};
    Race race;
    race.add(
        3,
        [&](size_t)
        {
            for (size_t i = 0; i < 20; ++i)
                counter.fetch_add(1);
        });
    race.run();
    EXPECT_EQ(counter.load(), 60u);
}

}

WYRD_TEST(Wyrd, SimulationSameSeedGivesTheSameRun, .runs = 1, .seed = 1)
{
    incrementInThreeThreads();
}

TEST(Wyrd, SimulationDifferentSeedsGiveDifferentSchedules)
{
    DB::SetWithMemoryTracking<UInt64> digests;
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

        Race race;
        race.add(
            1,
            [&](size_t)
            {
                std::lock_guard lock_first(first);
                std::lock_guard lock_second(second);
            });
        race.add(
            1,
            [&](size_t)
            {
                std::lock_guard lock_second(second);
                std::lock_guard lock_first(first);
            });
        race.run();
    };

    bool found = false;
    for (UInt64 seed = 1; seed <= 200 && !found; ++seed)
    {
        SimulationParams params;
        params.collect_timeline = true;
        params.seed = seed;
        SimulationResult result = Simulation(params).run(lock_in_opposite_order);
        ASSERT_NE(result.status, SimulationResult::Status::Exception) << result.exception_message;
        if (result.status == SimulationResult::Status::Deadlock)
        {
            found = true;
            EXPECT_NE(result.timeline.find("thread 2: futexWait at"), std::string::npos) << result.timeline;
            EXPECT_NE(result.timeline.find("thread 3: futexWait at"), std::string::npos) << result.timeline;
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
    params.max_steps = 50;

    SimulationResult result = Simulation(params).run(
        []
        {
            Atomic<size_t> spinner{0};
            while (true)
                spinner.fetch_add(1);
        });

    EXPECT_EQ(result.status, SimulationResult::Status::StepLimit);
    EXPECT_EQ(result.total_steps, 50u);
}

TEST(Wyrd, SimulationSwitchesAtAPreemptionPoint)
{
    SimulationParams params;
    params.collect_timeline = true;
    params.max_steps = 1000;
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
    EXPECT_NE(result.timeline.find("preemptionPoint"), std::string::npos);
}

TEST(Wyrd, SimulationHoldsAThreadInsideLockFreeScope)
{
    SimulationParams params;
    params.collect_timeline = true;
    params.lock_free_suspend_probability = 1.0;
    params.max_suspended_threads = 2;

    SimulationResult result = Simulation(params).run(
        []
        {
            Atomic<size_t> counter{0};

            Race race;
            race.add(
                3,
                [&](size_t)
                {
                    LockFreeScope scope;
                    for (size_t i = 0; i < 50; ++i)
                    {
                        counter.fetch_add(1);
                        progress();
                    }
                });
            race.run();

            EXPECT_EQ(counter.load(), 150u);
        });

    EXPECT_EQ(result.status, SimulationResult::Status::Ok);
    EXPECT_NE(result.timeline.find("held"), std::string::npos);
}

TEST(Wyrd, SimulationFindsBlockingInsideLockFreeScope)
{
    auto block_on_a_lock_free_peer = []
    {
        Atomic<UInt32> busy{0};
        Atomic<size_t> work{0};

        Race race;
        race.add(
            1,
            [&](size_t)
            {
                LockFreeScope scope;
                busy.store(1);
                for (size_t i = 0; i < 10; ++i)
                {
                    work.fetch_add(1);
                    progress();
                }
                busy.store(0);
                futexWakeAll(busy);
            });
        race.add(
            1,
            [&](size_t)
            {
                LockFreeScope scope;
                UInt32 value = busy.load();
                while (value == 1)
                    futexWaitFetch(busy, value);
            });
        race.run();
    };

    bool found = false;
    for (UInt64 seed = 1; seed <= 200 && !found; ++seed)
    {
        SimulationParams params;
        params.collect_timeline = true;
        params.seed = seed;
        params.lock_free_suspend_probability = 1.0;
        params.preempt_probability = 1.0;
        SimulationResult result = Simulation(params).run(block_on_a_lock_free_peer);
        if (result.status == SimulationResult::Status::Deadlock)
        {
            found = true;
            EXPECT_NE(result.timeline.find("held at"), std::string::npos) << result.timeline;
            EXPECT_NE(result.timeline.find("futexWait at"), std::string::npos) << result.timeline;
        }
    }

    EXPECT_TRUE(found);
}

WYRD_TEST(Wyrd, SimulationSpuriousFailureEveryTime, .runs = 1, .simulation = {.spurious_failure_probability = 1.0})
{
    Atomic<int> value{0};
    int expected = 0;
    EXPECT_FALSE(value.compare_exchange_weak(expected, 1));
    EXPECT_EQ(expected, 0);
    EXPECT_TRUE(value.compare_exchange_strong(expected, 1));

    Mutex mutex;
    EXPECT_FALSE(mutex.try_lock());
}

#endif
