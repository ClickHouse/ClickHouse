#pragma once

#include <IO/FetchMachine.h>

namespace DB
{

/// Drives `MachineBase` machines over the shared pool: owns the state writes
/// and the scheduling/release ordering, never step semantics. The executor is
/// its only client and the single concurrent owner on the foreground side, so
/// state transitions need no CAS beyond the step handle's queued-pickup race:
///   - executor owns the machine from construction to `schedule`;
///   - the pool worker owns it from step entry (`Running`) until it stores a
///     parked/terminal state and its handle resolves (the release edge);
///   - the executor owns it again after `tryCancelQueued` succeeded or
///     `waitReleased` returned.
class FetchMachineRunner
{
public:
    explicit FetchMachineRunner(std::shared_ptr<PrefetchThreadPool> pool_) : pool(std::move(pool_)) {}

    /// Schedule the machine's next step. `kind` labels the step (and picks the
    /// pool once a dedicated cache-write pool exists; today both kinds share
    /// the one pool). Returns false - machine parked `ParkedPoolFull`, still
    /// executor-owned, payload untouched - when the queue rejects it.
    bool schedule(std::shared_ptr<MachineBase> machine, StepKind kind);

    /// Revoke a still-queued step (the handle CAS): on success the worker
    /// provably never ran, the payload is untouched and the machine is
    /// executor-owned again (state `Cancelled`). False = the step is
    /// running or finished; use `waitReleased`.
    bool tryCancelQueued(MachineBase & machine);

    /// Ask a running step to wrap up at its next interrupt point.
    void requestInterrupt(MachineBase & machine) { machine.interrupt_requested.store(true); }

    /// Block until the scheduled/running step releases the machine (its handle
    /// resolves), establishing the happens-before edge over the machine's
    /// payload. Step-body exceptions land in `machine.failure`, not here.
    /// No-op when no step was ever scheduled.
    void waitReleased(MachineBase & machine);

private:
    std::shared_ptr<PrefetchThreadPool> pool;
};

}
