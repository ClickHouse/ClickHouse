#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

namespace DB
{

void registerThrottlerConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<ThrottlerConstraint>("bandwidth_limit");
}

}
