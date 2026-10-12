#include <Common/Scheduler/CurrentCPULease.h>

namespace DB
{

namespace
{
    /// The CPU lease of the currently-running executor worker thread, or nullptr.
    thread_local ISlotLease * current_cpu_lease = nullptr;
    /// Number of active CPULeaseParkGuard scopes on this thread (for nesting).
    thread_local size_t cpu_park_depth = 0;
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
        // Only the outermost guard on this thread parks; it unparks iff park() actually parked
        // (park() returns false if it was a no-op, e.g. the allocation is shutting down).
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
