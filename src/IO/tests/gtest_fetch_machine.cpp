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

/// Pool that rejects the first `reject_first` submissions (a full queue) and
/// runs later ones inline on the submitting thread, returning a resolved
/// handle - single-threaded determinism for reschedule tests.
class InlinePool : public PrefetchThreadPool
{
public:
    explicit InlinePool(size_t reject_first_) : PrefetchThreadPool(NoWorkers{}), reject_first(reject_first_) {}

    std::shared_ptr<JobHandle> submitJob(std::function<void()> task) override
    {
        if (reject_first > 0)
        {
            --reject_first;
            return nullptr;
        }
        task();
        return makeCompletedJobHandleForTest();
    }

private:
    size_t reject_first;
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

    ASSERT_TRUE(runner.schedule(m));
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

    ASSERT_TRUE(runner.schedule(m));
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

    EXPECT_FALSE(runner.schedule(m));
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
    ASSERT_TRUE(runner.schedule(m));

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
    ASSERT_TRUE(runner.schedule(m));

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

    ASSERT_TRUE(runner.schedule(m));
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
    ASSERT_TRUE(runner.schedule(m));

    started.wait();
    runner.requestInterrupt(*m);
    flagged.count_down();
    runner.waitReleased(*m);

    EXPECT_TRUE(observed.load());
}

TEST(FetchMachine, InterruptedResultParksAsInterrupted)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [] { return StepResult::Interrupted; };

    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Interrupted);
    EXPECT_EQ(m->failure, nullptr);
}

TEST(FetchMachine, FlagsAreCooperativeOnly)
{
    /// The runner never gates on the flag: a step that does not poll it runs
    /// to completion even when it was set before it was scheduled. Stopping
    /// early is entirely the step body's contract; only `tryCancelQueued`
    /// prevents a queued step from running at all.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [self = m.get()]
    {
        self->runs.fetch_add(1);
        return StepResult::Done;
    };

    runner.requestInterrupt(*m);
    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->runs.load(), 1u) << "the flag must not prevent a queued step from running";
    EXPECT_EQ(m->state.load(), MachineState::Done);
}

TEST(FetchMachine, ScheduleDoesNotResetStickyFlags)
{
    /// The flags are sticky and owner-cleared: neither `schedule` nor the
    /// release resets them. A machine re-armed for a second step clears the
    /// consumed takeover flag itself so the next step does not inherit it
    /// (see `schedulePutStep`).
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    std::atomic<bool> seen_at_entry{false};
    m->run_step = [&, self = m.get()]
    {
        seen_at_entry.store(self->interrupt_requested.load());
        return StepResult::Done;
    };

    runner.requestInterrupt(*m);
    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_TRUE(seen_at_entry.load());
    EXPECT_TRUE(m->interrupt_requested.load());
}

TEST(FetchMachine, TryCancelWithoutScheduleIsFalse)
{
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    EXPECT_FALSE(runner.tryCancelQueued(*m));
    EXPECT_EQ(m->state.load(), MachineState::Constructed);
}

TEST(FetchMachine, TryCancelFinishedIsFalse)
{
    /// The revoke verdict is definite both ways: false means a worker picked
    /// the step up - a revoke never un-runs a completed step or overwrites
    /// its final state.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [] { return StepResult::Done; };

    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_FALSE(runner.tryCancelQueued(*m));
    EXPECT_EQ(m->state.load(), MachineState::Done);
}

TEST(FetchMachine, RevokeIsOneShot)
{
    /// A second revoke after a successful one answers false (the CAS already
    /// fired) and the state stays Cancelled - the "never ran" guarantee
    /// cannot be claimed twice.
    auto pool = std::make_shared<PrefetchThreadPool>(1, /*queue_size=*/4);
    FetchMachineRunner runner(pool);

    std::latch worker_latch{1};
    auto blocker = pool->submitJob([&] { worker_latch.wait(); });
    ASSERT_NE(blocker, nullptr);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [] { return StepResult::Done; };
    ASSERT_TRUE(runner.schedule(m));

    EXPECT_TRUE(runner.tryCancelQueued(*m));
    EXPECT_FALSE(runner.tryCancelQueued(*m));
    EXPECT_EQ(m->state.load(), MachineState::Cancelled);

    worker_latch.count_down();
    blocker->get();
    runner.waitReleased(*m);
}

TEST(FetchMachine, ScheduleAgainAfterPoolFullPark)
{
    /// The reschedule ladder (`sweepPutMachines`): a ParkedPoolFull machine is
    /// still executor-owned and intact, so a later `schedule` simply retries.
    auto pool = std::make_shared<InlinePool>(/*reject_first=*/1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    m->run_step = [self = m.get()]
    {
        self->runs.fetch_add(1);
        return StepResult::Done;
    };

    EXPECT_FALSE(runner.schedule(m));
    EXPECT_EQ(m->state.load(), MachineState::ParkedPoolFull);

    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Done);
    EXPECT_EQ(m->runs.load(), 1u);
}

TEST(FetchMachine, ReleaseEdgePublishesFinalState)
{
    /// The ordering contract: the parked/terminal state is stored BEFORE the
    /// step's handle resolves, so a waiter never observes Scheduled/Running
    /// after `waitReleased`. Also pins that one machine can be re-armed and
    /// re-scheduled many times: each `schedule` installs a fresh handle, no
    /// manual reset between steps is required.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<CountingMachine>();
    for (size_t round = 0; round < 64; ++round)
    {
        const bool park = (round % 2 == 0);
        m->run_step = [park, self = m.get()]
        {
            self->runs.fetch_add(1);
            return park ? StepResult::AwaitCollect : StepResult::Done;
        };
        ASSERT_TRUE(runner.schedule(m));
        runner.waitReleased(*m);
        ASSERT_EQ(m->state.load(), park ? MachineState::AwaitCollect : MachineState::Done);
    }
    EXPECT_EQ(m->runs.load(), 64u);
}
