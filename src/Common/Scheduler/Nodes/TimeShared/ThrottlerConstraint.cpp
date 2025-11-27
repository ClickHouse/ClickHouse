#include <Common/Scheduler/Nodes/TimeShared/ThrottlerConstraint.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerThrottlerConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<ThrottlerConstraint>("bandwidth_limit");
}

}
