#pragma once

#include <Common/ISlotControl.h>

#include <functional>

namespace DB
{

/// Callbacks of the CPU lease (see `CPULeaseAllocation`), called with the slot id of a thread preempted by the
/// workload scheduler and of a thread resumed after preemption.
struct CPUSlotPreemptionCallbacks
{
    std::function<void(size_t slot_id)> on_preempt;
    std::function<void(size_t slot_id)> on_resume;
};

/// Allocates CPU slots for `num_threads` threads of the current query: from the workload scheduler if a CPU resource
/// is defined (`CREATE RESOURCE ... (MASTER THREAD | WORKER THREAD)`), from `ConcurrencyControl` otherwise.
/// The first slot, taken with `acquire`, is the one of the master thread, the others are taken with `tryAcquire`.
///
/// `lazy_allocation`: request only the slot of the master thread, the others are requested with `setMax`.
///
/// `blocking`: whether a thread may wait for a slot of the workload scheduler (`ConcurrencyControl` never makes it wait).
/// If true, the master thread waits: in `acquire` with `cpu_slot_preemption` disabled, otherwise in `renew` while it is
/// preempted, with no time limit. A preempted worker thread waits in `renew` for `cpu_slot_preemption_timeout_ms`.
/// If false, `acquire` returns at once, and `renew` of a preempted worker thread returns false at once.
/// The master thread must not call `renew` then: it would still wait.
/// The slots are CPU leases then, even with `cpu_slot_preemption` disabled: a lease is preempted only in `renew`.
SlotAllocationPtr allocateCPUSlots(size_t num_threads, bool lazy_allocation, bool blocking, CPUSlotPreemptionCallbacks callbacks = {});

}
