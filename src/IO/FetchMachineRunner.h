#pragma once

#include <IO/FetchMachine.h>

namespace DB
{

/// Drives `MachineBase` machines over the shared pool: owns the state writes
/// and the scheduling/release ordering, never step semantics. The executor is
/// the only client and the single concurrent owner on the foreground side, so
/// transitions need no CAS beyond the step handle's queued-pickup race:
///   - executor owns the machine from construction to `schedule`;
///   - the pool worker owns it from step entry (`Running`) until it stores a
///     parked/terminal state and its handle resolves (the release edge);
///   - the executor owns it again after `tryCancelQueued` succeeded or
///     `waitReleased` returned.
class FetchMachineRunner
{
public:
    explicit FetchMachineRunner(std::shared_ptr<PrefetchThreadPool> pool_);

    /// Schedule the machine's next step. Returns false - machine parked
    /// `ParkedPoolFull`, executor-owned, payload untouched - on queue reject.
    bool schedule(std::shared_ptr<MachineBase> machine);

    /// Revoke a still-queued step (the handle CAS): on success the worker
    /// provably never ran and the machine is executor-owned again
    /// (`Cancelled`). False = running or finished; use `waitReleased`.
    bool tryCancelQueued(MachineBase & machine);

    /// Ask a running step to wrap up at its next interrupt point.
    void requestInterrupt(MachineBase & machine);

    /// Block until the step releases the machine (its handle resolves),
    /// establishing the happens-before edge over the payload. Step-body
    /// exceptions land in `machine.failure`, not here. Idempotent: the
    /// consumed handle is dropped (a `std::future` must not be joined twice).
    void waitReleased(MachineBase & machine);

private:
    std::shared_ptr<PrefetchThreadPool> pool;
};

}
