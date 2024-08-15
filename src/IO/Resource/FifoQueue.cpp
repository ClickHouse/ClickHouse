#include <IO/Resource/FifoQueue.h>

#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerFifoQueue(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FifoQueue>("fifo");
}

}
