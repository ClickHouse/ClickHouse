#include <Common/Scheduler/Nodes/TimeShared/FairPolicy.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerFairPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FairPolicy>("fair");
}

}
