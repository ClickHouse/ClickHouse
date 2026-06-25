#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

namespace DB
{

void registerSemaphoreConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<SemaphoreConstraint>("inflight_limit");
}

}
