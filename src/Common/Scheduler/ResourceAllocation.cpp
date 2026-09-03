#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IAllocationQueue.h>

namespace DB
{

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
