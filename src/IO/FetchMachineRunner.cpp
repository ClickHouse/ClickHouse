#include <IO/FetchMachineRunner.h>

#include <base/defines.h>

namespace DB
{

FetchMachineRunner::FetchMachineRunner(std::shared_ptr<PrefetchThreadPool> pool_)
    : pool(std::move(pool_))
{
}

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
            /// The parked/terminal state must be stored BEFORE the handle
            /// resolves, so a waiter woken by the handle reads the final state.
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

    /// The worker provably never ran (Queued->Cancelled beat Queued->Running),
    /// so the payload is untouched and executor-owned again.
    machine.state.store(MachineState::Cancelled);
    return true;
}

void FetchMachineRunner::requestInterrupt(MachineBase & machine)
{
    machine.interrupt_requested.store(true);
}

void FetchMachineRunner::waitReleased(MachineBase & machine)
{
    if (!machine.current_step)
        return;
    /// Cannot throw: the schedule wrapper captures step-body exceptions into
    /// `machine.failure`, and a revoked step's handle resolves with a value.
    machine.current_step->get();
    /// Joined exactly once: drop the consumed handle.
    machine.current_step.reset();
}

}
