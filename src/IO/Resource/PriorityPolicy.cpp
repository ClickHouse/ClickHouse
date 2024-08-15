#include <IO/Resource/PriorityPolicy.h>

#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerPriorityPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<PriorityPolicy>("priority");
}

}
