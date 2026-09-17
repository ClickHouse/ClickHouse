#pragma once

#include <IO/FetchMachineRunner.h>

namespace DB
{

/// Inline runner: runs the machine's step SYNCHRONOUSLY on the calling (serve) thread, so the
/// foreground can drive a `FetchMachine` without the pool. The step is settled before
/// `schedule` returns; the machine never carries a `JobHandle` (`current_step` stays null), so
/// the collect verbs (`waitReleased` / `tryCancelQueued`) and the abandoned-machine drain all
/// no-op on it - structurally avoiding the double-join that a pool future would risk.
class LocalFetchMachineRunner : public IFetchMachineRunner
{
public:
    bool schedule(std::shared_ptr<MachineBase> machine) override;
    bool tryCancelQueued(MachineBase & machine) override;
    void requestInterrupt(MachineBase & machine) override;
    void waitReleased(MachineBase & machine) override;
};

}
