#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IAllocationConstraint.h>

#include <base/defines.h>

#include <ranges>

namespace DB
{

void ResourceAllocation::decrease(ResourceCost size)
{
    chassert(allocated_size > size);

    allocated_size -= size;

    // Iterate over constraints in reverse order
    for (ISchedulerConstraint * constraint : std::ranges::reverse_view(constraints))
    {
        if (constraint)
            static_cast<IAllocationConstraint*>(constraint)->decreaseAllocation(this, size);
    }
}

}
