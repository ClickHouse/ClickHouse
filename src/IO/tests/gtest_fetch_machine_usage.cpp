#include <IO/FetchMachineRunner.h>

#include <gtest/gtest.h>
#include <functional>
#include <latch>
#include <memory>
#include <vector>

using namespace DB;

/// Executable usage examples for the machine/runner protocol, modelled on how
/// `ReaderExecutor` drives real machines. The centerpiece is the shape of a
/// good step body:
///
///   - it works in bounded tiles and re-checks the stop request at every tile
///     boundary - a SAFE POINT, where all progress is already committed into
///     machine-owned fields (cursor advanced, partial product consistent,
///     nothing half-open);
///   - on a stop it parks itself by returning `StepResult::Interrupted`,
///     KEEPING the partial product - the executor decides its fate (today:
///     cancel discards it at the reap; a future takeover policy may collect);
///   - because the cursor lives in the machine, a re-armed step resumes from
///     where the previous one stopped: nothing is recomputed, nothing is lost.
///
/// Production counterpart: `ReaderExecutor::Connection::readInto` checks the
/// stop gate between source blocks; the rope and the connection frontier are
/// the committed progress.

namespace
{

/// A machine that "fetches" `total_tiles` tiles into `product`. `next_tile`
/// is the resume cursor; machine-owned, so private to the worker while a step
/// runs and safely readable by the foreground after the release edge.
struct TileMachine : MachineBase
{
    size_t total_tiles = 4;

    /// A step-body POLICY example: this step honors the stop only while the
    /// REMAINING work exceeds a cost bound (near the tail, completing beats
    /// wrapping). Production fetch steps choose differently per connection
    /// kind: live stops freely (the connection is saved and continues), a
    /// one-shot never stops mid-GET (stops land between connections). The
    /// caller is not waiting either way - cancel is a flag + the soft list.
    size_t breakeven_tiles = 1;

    size_t next_tile = 0;
    std::vector<size_t> product;

    /// Test rendezvous: before producing `pause_at_tile`, the step signals
    /// `reached` and parks on `resume`, so a test can set flags at a
    /// deterministic point of progress.
    size_t pause_at_tile = static_cast<size_t>(-1);
    std::latch reached{1};
    std::latch resume{1};
};

/// The reference interruptible step; the gate is the step's own policy.
std::function<StepResult()> makeTileStep(TileMachine & machine)
{
    return [self = &machine]
    {
        while (self->next_tile < self->total_tiles)
        {
            /// The safe point: nothing is half-done here, so wrapping up is
            /// just a return - the products stay consistent.
            const size_t remaining = self->total_tiles - self->next_tile;
            if (self->interrupt_requested.load() && remaining > self->breakeven_tiles)
                return StepResult::Interrupted;

            if (self->next_tile == self->pause_at_tile)
            {
                self->reached.count_down();
                self->resume.wait();
            }

            self->product.push_back(self->next_tile);
            ++self->next_tile;
        }
        return StepResult::Done;
    };
}

}

TEST(FetchMachineUsage, TakeoverSavesPartialWorkAndResumesLater)
{
    /// The takeover (`tryCollectMachine`): the foreground needs the bytes NOW,
    /// so instead of blocking for the whole window it asks the running step to
    /// wrap up at its next safe point, collects the partial product, and can
    /// later re-arm the machine to continue from the cursor.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<TileMachine>();
    m->total_tiles = 6;
    m->pause_at_tile = 2;
    m->run_step = makeTileStep(*m);

    ASSERT_TRUE(runner.schedule(m));
    m->reached.wait();  /// tiles {0, 1} are committed, tile 2 is in flight

    /// Request the takeover and join: the wait is bounded by one tile, not
    /// the whole window.
    runner.requestInterrupt(*m);
    m->resume.count_down();
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Interrupted);
    EXPECT_EQ(m->product, (std::vector<size_t>{0, 1, 2})) << "partial work saved, nothing lost";
    EXPECT_EQ(m->next_tile, 3u);

    /// Re-arm for the continuation (the production recipe, see
    /// `scheduleCacheFillStep`): clear the consumed takeover flag - the next
    /// step must not inherit it - and schedule again; the cursor already
    /// points at the first unproduced tile.
    m->interrupt_requested.store(false);
    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Done);
    EXPECT_EQ(m->product, (std::vector<size_t>{0, 1, 2, 3, 4, 5}));
}

TEST(FetchMachineUsage, InterruptWithinBreakevenCompletesInstead)
{
    /// The breakeven half of the gate: a takeover request landing when the
    /// remaining work is within the cost bound lets the step finish the window
    /// instead of wrapping - serving a tiny prefix is not worth shredding a
    /// streaming window. The waiter is still released promptly: the leftover
    /// is bounded by the breakeven by construction.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<TileMachine>();
    m->total_tiles = 4;
    m->breakeven_tiles = 4;  /// the remaining work never exceeds the bound
    m->pause_at_tile = 1;
    m->run_step = makeTileStep(*m);

    ASSERT_TRUE(runner.schedule(m));
    m->reached.wait();
    runner.requestInterrupt(*m);
    m->resume.count_down();
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Done) << "completed, not parked";
    EXPECT_EQ(m->product.size(), 4u);
}

TEST(FetchMachineUsage, CancelWhileQueuedExitsAtEntryWithEmptyProduct)
{
    /// When a cancel races the queue pickup and the revoke CAS loses, the
    /// flag still does its job: the FIRST safe point is before any tile, so
    /// the step exits Interrupted having produced nothing. There is no
    /// dedicated "never started" state - the empty product carries it, and
    /// the caller degrades to the revoke path (`tryCollectMachine` falls back
    /// to the sync read on an empty interrupted product).
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<TileMachine>();
    m->run_step = makeTileStep(*m);

    runner.requestInterrupt(*m);
    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Interrupted);
    EXPECT_TRUE(m->product.empty());
}

TEST(FetchMachineUsage, CancelLadderRevokeThenAbortAndJoin)
{
    /// The cancel recipe (`cancelMachine`): try the synchronous revoke first -
    /// its verdict is instant and definite. True: the step provably never ran
    /// and never will (the handle's Queued->Cancelled CAS beat the worker's
    /// Queued->Running), so the payload is untouched and reclaimable on the
    /// spot. False: a worker owns the machine - flag the work as doomed and
    /// join; the wait is bounded by one safe point, and the products are
    /// discarded by dropping the machine.
    auto pool = std::make_shared<PrefetchThreadPool>(1, /*queue_size=*/4);
    FetchMachineRunner runner(pool);

    /// Queued: the single worker is busy, the step cannot start.
    {
        std::latch busy{1};
        auto blocker = pool->submitJob([&] { busy.wait(); });
        ASSERT_NE(blocker, nullptr);

        auto m = std::make_shared<TileMachine>();
        m->run_step = makeTileStep(*m);
        ASSERT_TRUE(runner.schedule(m));

        ASSERT_TRUE(runner.tryCancelQueued(*m));
        EXPECT_EQ(m->state.load(), MachineState::Cancelled);
        EXPECT_TRUE(m->product.empty()) << "payload untouched - safe to reclaim synchronously";

        busy.count_down();
        blocker->get();
        runner.waitReleased(*m);  /// join the revoked step's no-op pickup before dropping the machine
    }

    /// Running: the revoke is too late - flag it and join.
    {
        auto m = std::make_shared<TileMachine>();
        m->total_tiles = 6;
        m->pause_at_tile = 1;
        m->run_step = makeTileStep(*m);
        ASSERT_TRUE(runner.schedule(m));

        m->reached.wait();  /// the step is mid-window now
        EXPECT_FALSE(runner.tryCancelQueued(*m));

        runner.requestInterrupt(*m);
        m->resume.count_down();
        runner.waitReleased(*m);

        EXPECT_EQ(m->state.load(), MachineState::Interrupted);
        /// The partial product exists but the work was doomed: the caller
        /// accounts it as wasted and drops the machine.
        EXPECT_EQ(m->product, (std::vector<size_t>{0, 1}));
    }
}

TEST(FetchMachineUsage, DeferredCancelParksOnSoftListJoinsLater)
{
    /// The delayed-cancel shape: flag the doomed step but do NOT block on it -
    /// stash the machine aside, keep doing foreground work, probe the handle's
    /// `isFinished` opportunistically (what `drainAbandonedMachines` does for
    /// revoked machines) and hard-join at the latest on teardown
    /// (`~ReaderExecutor` drains with wait_finished=true).
    ///
    /// Join exactly once: `waitReleased` consumes the handle's future - a
    /// second join on the same handle is undefined, and `isFinished` reads
    /// false again once consumed. Track joined-ness by dropping the machine
    /// from the list, never by re-probing after a join.
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    auto m = std::make_shared<TileMachine>();
    m->total_tiles = 6;
    m->pause_at_tile = 1;
    m->run_step = makeTileStep(*m);
    ASSERT_TRUE(runner.schedule(m));
    m->reached.wait();

    /// Cancel without blocking: flag it, stash it.
    runner.requestInterrupt(*m);
    std::vector<std::shared_ptr<TileMachine>> abandoned;
    abandoned.push_back(m);

    /// The soft probe: still parked at the rendezvous, provably not released.
    EXPECT_FALSE(m->current_step->isFinished());

    /// ... the foreground does useful work here instead of waiting ...

    m->resume.count_down();

    /// Teardown: the hard join. After it the machine is executor-owned again
    /// and safe to destroy.
    for (const auto & am : abandoned)
        runner.waitReleased(*am);
    abandoned.clear();

    EXPECT_EQ(m->state.load(), MachineState::Interrupted);
    EXPECT_EQ(m->product, (std::vector<size_t>{0, 1}));
}

TEST(FetchMachineUsage, BarrierThenReArmForThePutStep)
{
    /// The two-step machine (`scheduleCacheFillStep`): step one fetches and
    /// parks at the barrier (AwaitCollect - products ready, executor's move);
    /// the foreground collects, then re-arms the SAME machine with the put
    /// step. Every transition is executor-mediated - a worker never schedules
    /// the next step itself. The re-arm clears a consumed takeover flag (the
    /// fetch's interrupt must not leak into the put) and drops the resolved
    /// handle (a later pool-full park must not leave a stale joinable handle).
    auto pool = std::make_shared<PrefetchThreadPool>(1);
    FetchMachineRunner runner(pool);

    struct TwoPhaseMachine : MachineBase
    {
        std::vector<size_t> fetched;
        std::vector<size_t> cached;
    };

    auto m = std::make_shared<TwoPhaseMachine>();
    m->run_step = [self = m.get()]
    {
        self->fetched = {1, 2, 3};
        return StepResult::AwaitCollect;
    };
    ASSERT_TRUE(runner.schedule(m));

    /// A takeover racing the fetch's tail: consumed (or moot), but sticky.
    runner.requestInterrupt(*m);
    runner.waitReleased(*m);
    ASSERT_EQ(m->state.load(), MachineState::AwaitCollect);

    /// Collect: the release edge makes the products safely readable here.
    ASSERT_EQ(m->fetched, (std::vector<size_t>{1, 2, 3}));

    /// Re-arm: clear the inherited flag, drop the resolved handle, install
    /// the put step, schedule again.
    m->interrupt_requested.store(false);
    m->current_step.reset();
    m->run_step = [self = m.get()]
    {
        self->cached = self->fetched;
        /// A put step is stopped only by a cancel; the re-arm above cleared the
        /// fetch's consumed flag, so nothing leaks into the fill.
        return self->interrupt_requested.load() ? StepResult::Interrupted : StepResult::Done;
    };
    ASSERT_TRUE(runner.schedule(m));
    runner.waitReleased(*m);

    EXPECT_EQ(m->state.load(), MachineState::Done);
    EXPECT_EQ(m->cached, (std::vector<size_t>{1, 2, 3}));
    EXPECT_FALSE(m->interrupt_requested.load());
}
