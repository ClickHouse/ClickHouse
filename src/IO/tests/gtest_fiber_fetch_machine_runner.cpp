#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <IO/FetchMachine.h>
#include <IO/FiberFetchMachineRunner.h>
#include <IO/tests/gtest_silk_environment.h>

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

#include <atomic>
#include <stdexcept>

namespace
{

::testing::Environment * const silk_env = DB::tests::registerSilkEnvironment();

}

TEST(FiberFetchMachineRunner, RunsStepAndReleases)
{
    DB::FiberFetchMachineRunner runner;
    auto machine = std::make_shared<DB::MachineBase>();
    std::atomic<bool> ran{false};
    machine->run_step = [&ran]
    {
        ran.store(true);
        return DB::StepResult::AwaitCollect;
    };

    ASSERT_TRUE(runner.schedule(machine));
    runner.waitReleased(*machine);

    EXPECT_TRUE(ran.load());
    EXPECT_EQ(machine->state.load(), DB::MachineState::AwaitCollect);
    EXPECT_EQ(machine->failure, nullptr);
}

TEST(FiberFetchMachineRunner, CapturesStepFailure)
{
    DB::FiberFetchMachineRunner runner;
    auto machine = std::make_shared<DB::MachineBase>();
    machine->run_step = []() -> DB::StepResult
    {
        throw std::runtime_error("boom");
    };

    ASSERT_TRUE(runner.schedule(machine));
    runner.waitReleased(*machine);

    EXPECT_EQ(machine->state.load(), DB::MachineState::Failed);
    EXPECT_NE(machine->failure, nullptr);
}

TEST(FiberFetchMachineRunner, InterruptedStepStoresInterrupted)
{
    DB::FiberFetchMachineRunner runner;
    auto machine = std::make_shared<DB::MachineBase>();
    machine->run_step = [] { return DB::StepResult::Interrupted; };

    ASSERT_TRUE(runner.schedule(machine));
    runner.requestInterrupt(*machine);
    runner.waitReleased(*machine);

    EXPECT_TRUE(machine->interrupt_requested.load());
    EXPECT_EQ(machine->state.load(), DB::MachineState::Interrupted);
}

TEST(FiberFetchMachineRunner, TryCancelQueuedAlwaysFalse)
{
    DB::FiberFetchMachineRunner runner;
    auto machine = std::make_shared<DB::MachineBase>();
    machine->run_step = [] { return DB::StepResult::AwaitCollect; };

    ASSERT_TRUE(runner.schedule(machine));
    EXPECT_FALSE(runner.tryCancelQueued(*machine));
    runner.waitReleased(*machine);
}

TEST(FiberFetchMachineRunner, RunnerIsReusableAcrossMachines)
{
    DB::FiberFetchMachineRunner runner;
    for (int i = 0; i < 3; ++i)
    {
        auto machine = std::make_shared<DB::MachineBase>();
        machine->run_step = [] { return DB::StepResult::Done; };
        ASSERT_TRUE(runner.schedule(machine));
        runner.waitReleased(*machine);
        EXPECT_EQ(machine->state.load(), DB::MachineState::Done);
        /// Idempotent second join.
        runner.waitReleased(*machine);
    }
}

#endif
