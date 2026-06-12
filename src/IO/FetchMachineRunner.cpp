#include <IO/FetchMachineRunner.h>

#include <base/defines.h>

namespace DB
{

bool FetchMachineRunner::schedule(std::shared_ptr<MachineBase> machine)
{
    chassert(machine && machine->run_step);

    /// Scheduled is stored BEFORE submit: the worker may start the instant
    /// submit returns, and its first action is the Running store.
    machine->state.store(MachineState::Scheduled);

    auto handle = pool->submitJob([m = machine]
    {
        /// A revoked step never reaches here (the handle's queued-pickup CAS
        /// no-ops the job), so this entry IS the ownership transfer.
        m->state.store(MachineState::Running);
        try
        {
            const StepResult result = m->run_step();
            /// Store the parked/terminal state BEFORE this job resolves its
            /// handle (set_value runs after this lambda returns), so a waiter
            /// woken by the handle reads the final state.
            switch (result)
            {
                case StepResult::AwaitCollect:
                    m->state.store(MachineState::AwaitCollect);
                    break;
                case StepResult::Interrupted:
                    m->state.store(MachineState::Interrupted);
                    break;
                case StepResult::Done:
                    m->state.store(MachineState::Done);
                    break;
            }
        }
        catch (...)
        {
            m->failure = std::current_exception();
            m->state.store(MachineState::Failed);
        }
    });

    if (!handle)
    {
        machine->state.store(MachineState::ParkedPoolFull);
        return false;
    }

    machine->current_step = std::move(handle);
    return true;
}

bool FetchMachineRunner::tryCancelQueued(MachineBase & machine)
{
    if (!machine.current_step || !machine.current_step->tryCancel())
        return false;

    /// The worker provably never ran (the CAS Queued->Cancelled beat
    /// Queued->Running), so the payload is untouched and executor-owned again.
    machine.state.store(MachineState::Cancelled);
    return true;
}

void FetchMachineRunner::waitReleased(MachineBase & machine)
{
    if (!machine.current_step)
        return;
    try
    {
        machine.current_step->get();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Only a revoked step's handle throws here ("task was cancelled");
        /// step-body exceptions are captured into `machine.failure` by the
        /// schedule wrapper. Either way the handle resolved - the release
        /// (happens-before) edge over the payload is established.
    }
    /// Joined exactly once: drop the consumed handle (see the header note).
    machine.current_step.reset();
}

}
