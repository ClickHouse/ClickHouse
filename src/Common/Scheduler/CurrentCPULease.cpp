#include <Common/Scheduler/CurrentCPULease.h>

#include <utility>

namespace DB
{

namespace
{
    /// The CPU lease of the currently-running executor worker thread, or nullptr.
    thread_local ISlotLease * current_cpu_lease = nullptr;
    /// Number of active CPULeaseParkGuard scopes on this thread (for nesting).
    thread_local size_t cpu_park_depth = 0;
    /// The CPU slot that the current thread executes processors on, or nullptr.
    thread_local IAcquiredSlot * current_cpu_slot = nullptr;
}

IAcquiredSlot * getCurrentCPUSlot()
{
    return current_cpu_slot;
}

CurrentCPUSlotScope::CurrentCPUSlotScope(IAcquiredSlot * slot)
    : previous_slot(std::exchange(current_cpu_slot, slot))
{
}

CurrentCPUSlotScope::~CurrentCPUSlotScope()
{
    current_cpu_slot = previous_slot;
}

ISlotLease * getCurrentCPULease()
{
    return current_cpu_lease;
}

void setCurrentCPULease(ISlotLease * lease)
{
    current_cpu_lease = lease;
}

CPULeaseParkGuard::CPULeaseParkGuard()
    : lease(current_cpu_lease)
    , parked(false)
{
    if (lease)
    {
        // Only the outermost guard parks, and it unparks only if `park` succeeded
        if (cpu_park_depth == 0)
            parked = lease->park();
        ++cpu_park_depth;
    }
}

CPULeaseParkGuard::~CPULeaseParkGuard()
{
    if (lease)
    {
        --cpu_park_depth;
        if (parked)
            lease->unpark();
    }
}

}
