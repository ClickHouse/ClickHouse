#pragma once

#include "config.h"

#if USE_SILK

#include <IO/FetchMachineRunner.h>

#include <silk/fibers/future.h>

namespace DB
{

/// Fiber-backed runner: the step runs as a Silk fiber on the server-wide
/// scheduler; a read on an empty socket suspends the fiber instead of
/// blocking an OS thread. Ownership protocol matches `PoolFetchMachineRunner`
/// with two differences: fibers start eagerly, so there is no revocable
/// queued window (`tryCancelQueued` is always false), and the release edge is
/// a `FiberFuture` (waitable from plain threads via a proxy fiber) instead of
/// a `JobHandle`. Drives at most one machine at a time - exactly the
/// executor's contract (one in-flight machine per executor).
class FiberFetchMachineRunner : public IFetchMachineRunner
{
public:
    bool schedule(std::shared_ptr<MachineBase> machine) override;
    bool tryCancelQueued(MachineBase & machine) override;
    void requestInterrupt(MachineBase & machine) override;
    void waitReleased(MachineBase & machine) override;

private:
    /// Release edge of the in-flight fiber; reset after each consumed wait.
    silk::FiberFuture step_released;
    /// The machine the fiber was launched for; null when nothing is in flight.
    MachineBase * inflight = nullptr;
};

}

#endif
