#include <IO/LocalFetchMachineRunner.h>

#include <base/defines.h>

namespace DB
{

bool LocalFetchMachineRunner::schedule(std::shared_ptr<MachineBase> machine)
{
    chassert(machine && machine->run_step);

    /// Run the step inline on the calling thread. Mirror the pool wrapper's state writes and
    /// exception capture so the collect path is identical: a step-body throw lands in
    /// `machine->failure` (rethrown at the single collect site), NOT out of `schedule`.
    machine->state.store(MachineState::Scheduled);
    machine->state.store(MachineState::Running);
    try
    {
        switch (machine->run_step())
        {
            case StepResult::AwaitCollect:
                machine->state.store(MachineState::AwaitCollect);
                break;
            case StepResult::Interrupted:
                machine->state.store(MachineState::Interrupted);
                break;
            case StepResult::Done:
                machine->state.store(MachineState::Done);
                break;
        }
    }
    catch (...)
    {
        machine->failure = std::current_exception();
        machine->state.store(MachineState::Failed);
    }

    /// Leave `current_step` null: the step already ran and settled on this thread. `waitReleased`
    /// and `tryCancelQueued` therefore no-op on it, so the inline machine never holds a future and
    /// can never be joined twice.
    return true;
}

bool LocalFetchMachineRunner::tryCancelQueued(MachineBase &)
{
    /// Nothing is ever queued - the step ran synchronously inside `schedule`.
    return false;
}

void LocalFetchMachineRunner::requestInterrupt(MachineBase & machine)
{
    /// Harmless: by the time anyone could ask, the inline step has already finished. Set the
    /// flag anyway to keep the protocol uniform with the pool runner.
    machine.interrupt_requested.store(true);
}

void LocalFetchMachineRunner::waitReleased(MachineBase &)
{
    /// Already settled inline in `schedule`; there is no handle to join.
}

}
