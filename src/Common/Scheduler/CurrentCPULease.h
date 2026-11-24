#pragma once

#include <Common/ISlotControl.h>

namespace DB
{

/// The CPU lease the current thread runs on: an executor thread (published by `WorkloadResources`) or index analysis.
/// `CPULeaseParkGuard` parks it, if parking is enabled, deep in a processor's `work` or in the executor's idle wait.

ISlotLease * getCurrentCPULease();
void setCurrentCPULease(ISlotLease * lease);

/// RAII: parks the current thread's CPU lease (if any and parkable) for a non-CPU wait and unparks it on scope exit.
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
