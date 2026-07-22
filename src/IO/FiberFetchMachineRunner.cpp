#include <IO/FiberFetchMachineRunner.h>

#if USE_SILK

#include <IO/SilkFiberJob.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>

#include <cstddef>

namespace DB
{

namespace
{

struct FiberStepParams
{
    SilkFiberJobHeader header;
    std::shared_ptr<MachineBase> machine;
};

/// The global fiber-switch hooks read `getFiberParameters` as a `SilkFiberJobHeader *`
/// without knowing about `FiberStepParams` - the header must be the first member, at
/// offset 0. `FiberStepParams` remains standard-layout (verified: `shared_ptr`'s own
/// layout does not depend on whether the pointee is polymorphic), so `offsetof` is
/// well-defined here, not merely conditionally-supported.
static_assert(offsetof(FiberStepParams, header) == 0, "fiber-switch hooks blind-cast getFiberParameters to SilkFiberJobHeader");

/// Mirrors the pool wrapper's state writes and exception capture, on a fiber
/// stack. The fiber entry point is noexcept: every C++ exception must be
/// caught here and stored on the machine.
int stepFiberMain(FiberStepParams * params) noexcept
{
    MachineBase & m = *params->machine;
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
    return 0;
}

}

bool FiberFetchMachineRunner::schedule(std::shared_ptr<MachineBase> machine)
{
    chassert(machine && machine->run_step);
    chassert(!inflight);

    /// Scheduled is stored BEFORE the fiber is spawned: it may start the
    /// instant `run` returns, and its first action is the Running store.
    machine->state.store(MachineState::Scheduled);
    inflight = machine.get();

    const int rc = silk::FiberScheduler::run(
        stepFiberMain,
        FiberStepParams{{getCurrentThreadGroup()}, machine},
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
