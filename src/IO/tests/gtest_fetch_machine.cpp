#include <IO/FetchMachineRunner.h>

#include <gtest/gtest.h>
#include <atomic>
#include <latch>
#include <memory>
#include <stdexcept>

using namespace DB;

namespace
{

/// Minimal machine for protocol tests: counts step runs.
struct CountingMachine : MachineBase
{
    std::atomic<size_t> runs{0};
};

/// Pool whose submitJob always reports a full queue.
class FullPool : public PrefetchThreadPool
{
public:
    FullPool() : PrefetchThreadPool(NoWorkers{}) {}
    std::shared_ptr<JobHandle> submitJob(std::function<void()> /*task*/) override { return nullptr; }
};

}

TEST(FetchMachine, ScheduleRunsStepAndParksAtBarrier)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [self = m.get()]
    {
        self->runs.fetch_add(1);
        return StepResult::AwaitCollect;
    };

    ASSERT_TRUE(runner.schedule(m, StepKind::Fetch));
    runner.waitReleased(*m);

    EXPECT_EQ(m->runs.load(), 1u);
    EXPECT_EQ(m->state.load(), MachineState::AwaitCollect);
    EXPECT_EQ(m->failure, nullptr);
}

TEST(FetchMachine, StepResultDoneReachesTerminalState)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [] { return StepResult::Done; };

    ASSERT_TRUE(runner.schedule(m, StepKind::Put));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Done);
}

TEST(FetchMachine, PoolFullParksMachineExecutorOwned)
{
    auto pool = std::make_shared<FullPool>();
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [self = m.get()]
    {
        self->runs.fetch_add(1);
        return StepResult::Done;
    };

    EXPECT_FALSE(runner.schedule(m, StepKind::Fetch));
    EXPECT_EQ(m->state.load(), MachineState::ParkedPoolFull);
    EXPECT_EQ(m->current_step, nullptr);
    EXPECT_EQ(m->runs.load(), 0u) << "a rejected schedule must not run the step";

    /// waitReleased on a never-scheduled machine is a no-op, not a hang.
    runner.waitReleased(*m);
}

TEST(FetchMachine, CancelQueuedReclaimsUntouchedMachine)
{
    /// Single worker blocked by another job; the machine's step stays Queued,
    /// so the revoke CAS wins and the payload provably never ran.
    auto pool = std::make_shared<PrefetchThreadPool>(1, /*queue_size=*/4);
    FetchMachineRunner runner(pool);

    std::latch worker_latch{1};
    auto blocker = pool->submitJob([&] { worker_latch.wait(); });
    ASSERT_NE(blocker, nullptr);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [self = m.get()]
    {
        self->runs.fetch_add(1);
        return StepResult::AwaitCollect;
    };
    ASSERT_TRUE(runner.schedule(m, StepKind::Fetch));

    EXPECT_TRUE(runner.tryCancelQueued(*m));
    EXPECT_EQ(m->state.load(), MachineState::Cancelled);

    /// Drain the pool through the revoked job's no-op pickup.
    worker_latch.count_down();
    blocker->get();
    runner.waitReleased(*m);  /// the cancelled handle resolves with the known exception, swallowed

    EXPECT_EQ(m->runs.load(), 0u) << "a revoked step must never run";
}

TEST(FetchMachine, CancelRunningFailsAndReleaseIsWaited)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    std::latch started{1};
    std::latch release{1};

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [&]
    {
        started.count_down();
        release.wait();
        return StepResult::AwaitCollect;
    };
    ASSERT_TRUE(runner.schedule(m, StepKind::Fetch));

    started.wait();
    EXPECT_EQ(m->state.load(), MachineState::Running);
    EXPECT_FALSE(runner.tryCancelQueued(*m)) << "a running step cannot be revoked";

    release.count_down();
    runner.waitReleased(*m);
    EXPECT_EQ(m->state.load(), MachineState::AwaitCollect);
}

TEST(FetchMachine, StepExceptionLandsInFailureNotInWait)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = []() -> StepResult { throw std::runtime_error("step failed"); };

    ASSERT_TRUE(runner.schedule(m, StepKind::Fetch));
    /// waitReleased must NOT rethrow - the failure is the executor's to
    /// rethrow at collect (fetch) or log (put).
    EXPECT_NO_THROW(runner.waitReleased(*m));

    EXPECT_EQ(m->state.load(), MachineState::Failed);
    ASSERT_NE(m->failure, nullptr);
    EXPECT_THROW(std::rethrow_exception(m->failure), std::runtime_error);
}

TEST(FetchMachine, InterruptFlagVisibleInsideStep)
{
    /// The cooperative stop request: a step that polls `interrupt_requested`
    /// observes a flag set after it started (the stage-2 interrupt-point
    /// contract; here only the visibility half is pinned).
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    std::latch started{1};
    std::latch flagged{1};

    auto m = std::make_shared<CountingMachine>();
    std::atomic<bool> observed{false};
    m->run_step = [&, self = m.get()]
    {
        started.count_down();
        flagged.wait();
        observed.store(self->interrupt_requested.load());
        return StepResult::AwaitCollect;
    };
    ASSERT_TRUE(runner.schedule(m, StepKind::Fetch));

    started.wait();
    runner.requestInterrupt(*m);
    flagged.count_down();
    runner.waitReleased(*m);

    EXPECT_TRUE(observed.load());
}
