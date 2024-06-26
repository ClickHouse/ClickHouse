#include <IO/Resource/registerSchedulerNodes.h>

#include <IO/ISchedulerNode.h>
#include <IO/ISchedulerConstraint.h>
#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerPriorityPolicy(SchedulerNodeFactory &);
void registerFairPolicy(SchedulerNodeFactory &);
void registerSemaphoreConstraint(SchedulerNodeFactory &);
void registerFifoQueue(SchedulerNodeFactory &);

void registerSchedulerNodes()
{
    auto & factory = SchedulerNodeFactory::instance();

    // ISchedulerNode
    registerPriorityPolicy(factory);
    registerFairPolicy(factory);

    // ISchedulerConstraint
    registerSemaphoreConstraint(factory);

    // ISchedulerQueue
    registerFifoQueue(factory);
}

}
