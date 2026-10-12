#pragma once

#include <Common/ISlotControl.h>

namespace DB
{

/// Thread-local access to the CPU lease of the currently-running executor worker thread.
///
/// A worker thread publishes its lease for the duration of its execution loop (see
/// `WorkloadResources` in the pipeline executor), so code deep inside a processor's `work()`
/// (blocking I/O) or the executor's idle wait can PARK the lease — releasing the CPU slot while
/// the thread is not using CPU — through `CPULeaseParkGuard`, without threading the lease pointer
/// through every call.
/// Everything here is a no-op when the current thread has no CPU lease (e.g. the concurrency
/// control path, non-preemptive slots, or a thread that is not an executor worker).

ISlotLease * getCurrentCPULease();
void setCurrentCPULease(ISlotLease * lease);

/// RAII: parks the current thread's CPU lease (if any) for a non-CPU wait — blocking I/O, or an
/// idle worker sleeping while there is no task — and unparks it on scope exit. Nesting-safe: if
/// several guards are active on one thread, only the outermost actually parks and unparks.
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
