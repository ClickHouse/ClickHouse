#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IAllocationQueue.h>

namespace DB
{

ResourceAllocation::ResourceAllocation(IAllocationQueue & queue_, const String & id_)
    : ResourceAllocation(queue_, id_, MemoryPressurePolicy{})
{
}

ResourceAllocation::ResourceAllocation(
    IAllocationQueue & queue_,
    const String & id_,
    MemoryPressurePolicy memory_pressure_policy_)
    : queue(queue_)
    , id(id_)
    , memory_pressure_policy(memory_pressure_policy_)
    , increase(*this)
    , decrease(*this)
{
}

ResourceAllocation::~ResourceAllocation()
{
    chassert(!pending_hook.is_linked());
    chassert(!running_hook.is_linked());
    chassert(!increasing_hook.is_linked());
    chassert(!decreasing_hook.is_linked());
    chassert(!removing_hook.is_linked());
    chassert(allocated == 0);
}

}
