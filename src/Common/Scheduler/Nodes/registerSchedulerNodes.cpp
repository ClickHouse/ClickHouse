#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/ISchedulerConstraint.h>
#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerPriorityPolicy(SchedulerNodeFactory &);
void registerFairPolicy(SchedulerNodeFactory &);
void registerSemaphoreConstraint(SchedulerNodeFactory &);
void registerThrottlerConstraint(SchedulerNodeFactory &);
void registerFifoQueue(SchedulerNodeFactory &);

void registerSchedulerNodes()
{
    auto & factory = SchedulerNodeFactory::instance();

    // ISchedulerNode
    registerPriorityPolicy(factory);
    registerFairPolicy(factory);

    // ISchedulerConstraint
    registerSemaphoreConstraint(factory);
    registerThrottlerConstraint(factory);

    // ISchedulerQueue
    registerFifoQueue(factory);
}

}
