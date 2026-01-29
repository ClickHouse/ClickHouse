#include <Common/Scheduler/Nodes/TimeShared/PriorityPolicy.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerPriorityPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<PriorityPolicy>("priority");
}

}
