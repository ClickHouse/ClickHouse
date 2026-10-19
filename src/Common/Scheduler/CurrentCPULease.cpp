#include <Common/Scheduler/CurrentCPULease.h>
#include <Common/FiberLocal.h>

namespace DB
{

/// Both are private to a stackful coroutine: it can suspend inside a nested pipeline and be resumed or unwound on another thread.
namespace
{
    /// The CPU lease of the currently-running executor worker thread, or nullptr.
    constinit FiberLocal<ISlotLease *, FiberLocalSlot::CURRENT_CPU_LEASE> current_cpu_lease;
    /// Number of active CPULeaseParkGuard scopes on this thread (for nesting).
    constinit FiberLocal<size_t, FiberLocalSlot::CPU_LEASE_PARK_DEPTH> cpu_park_depth;
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
        cpu_park_depth = cpu_park_depth + 1;
    }
}

CPULeaseParkGuard::~CPULeaseParkGuard()
{
    if (lease)
    {
        cpu_park_depth = cpu_park_depth - 1;
        if (parked)
            lease->unpark();
    }
}

ScopedCPULease::ScopedCPULease(ISlotLease * lease)
    : previous(current_cpu_lease)
{
    current_cpu_lease = lease;
}

ScopedCPULease::~ScopedCPULease()
{
    current_cpu_lease = previous;
}

}
