#pragma once

#include <Common/ISlotControl.h>

namespace DB
{

/// The CPU lease of the current executor thread, published by `WorkloadResources` in the pipeline executor,
/// so `CPULeaseParkGuard` can park it deep in a processor's `work` or in the executor's idle wait.
/// No-op when the thread has no CPU lease.

ISlotLease * getCurrentCPULease();
void setCurrentCPULease(ISlotLease * lease);

IAcquiredSlot * getCurrentCPUSlot();

/// RAII: publishes `slot` as the CPU slot of the current thread, and restores the previous one on scope exit.
class CurrentCPUSlotScope
{
public:
    explicit CurrentCPUSlotScope(IAcquiredSlot * slot);
    ~CurrentCPUSlotScope();
    CurrentCPUSlotScope(const CurrentCPUSlotScope &) = delete;
    CurrentCPUSlotScope & operator=(const CurrentCPUSlotScope &) = delete;

private:
    IAcquiredSlot * const previous_slot;
};

/// RAII: parks the current thread's CPU lease (if any) for a non-CPU wait and unparks it on scope exit.
/// Nesting-safe: only the outermost guard parks.
class CPULeaseParkGuard
{
public:
    CPULeaseParkGuard();
    ~CPULeaseParkGuard();
    CPULeaseParkGuard(const CPULeaseParkGuard &) = delete;
    CPULeaseParkGuard & operator=(const CPULeaseParkGuard &) = delete;

private:
    ISlotLease * const lease; /// The lease captured at construction (paired park/unpark target).
    bool parked; /// true iff this guard is the outermost one that actually parked.
};

}
