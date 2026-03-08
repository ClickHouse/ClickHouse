#include <Common/Scheduler/Nodes/TimeShared/FifoQueue.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB
{

void registerFifoQueue(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FifoQueue>("fifo");
}

}
