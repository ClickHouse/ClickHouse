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
/// The first acquired slot is the one of the master thread: it is never downscaled, and `acquire` provides it without a wait,
/// unless a separate `MASTER THREAD` resource is defined without preemption.
/// With `lazy_allocation` only the slot of the master thread is requested, the others are requested by `setMax`.
SlotAllocationPtr allocateCPUSlots(size_t num_threads, bool lazy_allocation, CPUSlotPreemptionCallbacks callbacks = {});

}
