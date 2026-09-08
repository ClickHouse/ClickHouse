#include <IO/FiberFetchMachineRunner.h>

#if USE_SILK

#include <IO/SilkFiberJob.h>

#include <Common/ThreadGroupSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>

#include <optional>

namespace DB
{

namespace
{

struct FiberStepParams
{
    SilkFiberJobHeader header;
    std::shared_ptr<MachineBase> machine;
    ThreadGroupPtr thread_group;
};

/// The global fiber-switch hooks read `getFiberParameters` as a `SilkFiberJobHeader *`
/// without knowing about `FiberStepParams` - the header must be the first member, at
/// offset 0. `FiberStepParams` remains standard-layout (verified: `shared_ptr`'s own
/// layout does not depend on whether the pointee is polymorphic), so `offsetof` is
/// well-defined here, not merely conditionally-supported. Enforced by `runSilkFiber`'s
/// static_assert at the spawn site below.

/// Mirrors the pool wrapper's state writes and exception capture, on a fiber
/// stack. The fiber entry point is noexcept: every C++ exception must be
/// caught here and stored on the machine.
int stepFiberMain(FiberStepParams * params) noexcept
{
    MachineBase & m = *params->machine;
    try
    {
        /// The fiber owns this ThreadStatus for the whole step: created here and destroyed
        /// before this function returns, one per step, rather than a carrier-OS-thread-wide
        /// ThreadStatus shared and lazily created across every fiber that thread ever borrows.
        /// `NoOSThreadTag` marks it as not owning a dedicated OS thread: `ThreadStatusExt.cpp`
        /// gates OS-thread-only bookkeeping (query profiler, perf counters, nice value, the
        /// group's OS-thread linked list) off `thread_id == NO_OS_THREAD`. The fiber-switch
        /// hooks (`SilkScheduler.cpp`) only ever swap `DB::current_thread` as this fiber
        /// migrates across carrier OS threads; they never construct or attach anything.
        ThreadStatus thread_status(ThreadStatus::NoOSThreadTag{});

        /// Attach to the submitter's group for the step's duration, so memory accounting and
        /// per-user throttling follow the fiber. A null group (e.g. a machine scheduled off
        /// any query) leaves the switcher a no-op, which is still correct: `thread_status`
        /// above keeps `current_thread` non-null for the whole step regardless. No thread
        /// name to set - fibers migrate across carrier OS threads too fast for a `setThreadName`
        /// syscall on each switch to mean anything.
        ThreadGroupSwitcher switcher(params->thread_group, std::nullopt);

        m.state.store(MachineState::Running);
        try
        {
            switch (m.run_step())
            {
                case StepResult::AwaitCollect:
                    m.state.store(MachineState::AwaitCollect);
                    break;
                case StepResult::Interrupted:
                    m.state.store(MachineState::Interrupted);
                    break;
                case StepResult::Done:
                    m.state.store(MachineState::Done);
                    break;
            }
        }
        catch (...)
        {
            m.failure = std::current_exception();
            m.state.store(MachineState::Failed);
        }
    }
    catch (...)
    {
        /// ThreadStatus construction itself can throw (e.g. an ErrnoException from the
        /// alt-stack setup) - and its constructor publishes `current_thread = this` BEFORE
        /// that throwing tail, so restore the invariant first: everything below (the machine
        /// teardown included) allocates through `current_thread`. At fiber-body entry the
        /// value is always null (the first-resume swap installs the header's zero slot), so
        /// null is the correct restore. This is also a fiber entry point: letting a C++
        /// exception escape it would call std::terminate, so fail the step instead.
        current_thread = nullptr;
        m.failure = std::current_exception();
        m.state.store(MachineState::Failed);
    }

    /// Drop our co-ownership INSIDE the fiber, after the ThreadStatus above is gone: if this
    /// copy is the last reference (the collector can finish the instant the future resolves,
    /// before the scheduler frees the parameter block), the machine's
    /// destructor - which may tear down an HTTPS connection and do socket
    /// I/O - runs in a suspendable fiber context, never on the bare carrier
    /// thread where a proxy-fiber wait on this CPU's own ring would deadlock.
    params->machine.reset();
    return 0;
}

}

bool FiberFetchMachineRunner::schedule(std::shared_ptr<MachineBase> machine)
{
    chassert(machine && machine->run_step);
    chassert(!inflight);

    /// Scheduled is stored BEFORE the fiber is spawned: the fiber may start the
    /// instant `run` returns (it sets up its ThreadStatus, then stores Running).
    machine->state.store(MachineState::Scheduled);
    inflight = machine.get();

    const int rc = runSilkFiber(
        stepFiberMain,
        FiberStepParams{{}, machine, getCurrentThreadGroup()},
        SilkFiberCategory::FETCH,
        &step_released);

    if (rc != 0)
    {
        /// ENOMEM from the fiber allocator: park like a full pool queue so
        /// the caller falls back to a synchronous read.
        inflight = nullptr;
        machine->state.store(MachineState::ParkedPoolFull);
        return false;
    }
    return true;
}

bool FiberFetchMachineRunner::tryCancelQueued(MachineBase &)
{
    /// Fibers start eagerly - there is no revocable queued window. The cancel
    /// path degrades to `requestInterrupt` + `waitReleased`, same as a
    /// picked-up pool task. Always returning `false` also means the executor
    /// never stashes a fiber machine into `abandoned_machines` (only a `true`
    /// return does that), so fiber machines never reach
    /// `drainAbandonedMachines`'s `current_step` access.
    return false;
}

void FiberFetchMachineRunner::requestInterrupt(MachineBase & machine)
{
    machine.interrupt_requested.store(true);
}

void FiberFetchMachineRunner::waitReleased(MachineBase & machine)
{
    /// Idempotent: only the in-flight machine has an unconsumed release edge.
    if (inflight != &machine)
        return;
    step_released.wait();
    step_released.reset();
    inflight = nullptr;
}

}

#endif
