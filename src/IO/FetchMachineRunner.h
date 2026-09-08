#pragma once

#include <IO/FetchMachine.h>

namespace DB
{

/// Drives `MachineBase` machines: owns the state writes and the scheduling/release
/// ordering, never step semantics. Implementations differ only in HOW the step runs -
/// `PoolFetchMachineRunner` submits it to the shared pool (async); a local runner runs
/// it inline on the calling thread (sync). The executor is the only client and the
/// single concurrent owner on the foreground side.
class IFetchMachineRunner
{
public:
    virtual ~IFetchMachineRunner() = default;

    /// Schedule the machine's next step. Returns false - machine parked
    /// `ParkedPoolFull`, executor-owned, payload untouched - on queue reject.
    virtual bool schedule(std::shared_ptr<MachineBase> machine) = 0;

    /// Revoke a still-queued step (the handle CAS): on success the worker
    /// provably never ran and the machine is executor-owned again
    /// (`Cancelled`). False = running or finished; use `waitReleased`.
    virtual bool tryCancelQueued(MachineBase & machine) = 0;

    /// Ask a running step to wrap up at its next interrupt point.
    virtual void requestInterrupt(MachineBase & machine) = 0;

    /// Block until the step releases the machine (its handle resolves),
    /// establishing the happens-before edge over the payload. Step-body
    /// exceptions land in `machine.failure`, not here. Idempotent: the
    /// consumed handle is dropped (a `std::future` must not be joined twice).
    virtual void waitReleased(MachineBase & machine) = 0;
};

/// Pool-backed runner: the step runs on the shared `PrefetchThreadPool` (async). Ownership
/// hand-off:
///   - executor owns the machine from construction to `schedule`;
///   - the pool worker owns it from step entry (`Running`) until it stores a
///     parked/terminal state and its handle resolves (the release edge);
///   - the executor owns it again after `tryCancelQueued` succeeded or
///     `waitReleased` returned.
/// Transitions need no CAS beyond the step handle's queued-pickup race.
class PoolFetchMachineRunner : public IFetchMachineRunner
{
public:
    explicit PoolFetchMachineRunner(std::shared_ptr<PrefetchThreadPool> pool_);

    bool schedule(std::shared_ptr<MachineBase> machine) override;
    bool tryCancelQueued(MachineBase & machine) override;
    void requestInterrupt(MachineBase & machine) override;
    void waitReleased(MachineBase & machine) override;

private:
    std::shared_ptr<PrefetchThreadPool> pool;
};

}
