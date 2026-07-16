#include <Common/Scheduler/Nodes/TimeShared/PriorityPolicy.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

namespace DB
{

void registerPriorityPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<PriorityPolicy>("priority");
}

}
