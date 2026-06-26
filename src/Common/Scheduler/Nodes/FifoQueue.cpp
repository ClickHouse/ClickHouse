#include <Common/Scheduler/Nodes/FifoQueue.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>

namespace DB
{

void registerFifoQueue(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FifoQueue>("fifo");
}

}
