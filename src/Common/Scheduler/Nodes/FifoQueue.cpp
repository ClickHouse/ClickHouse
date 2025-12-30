#include <Common/Scheduler/Nodes/FifoQueue.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerFifoQueue(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FifoQueue>("fifo");
}

}
