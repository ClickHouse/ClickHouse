#include <IO/Resource/ThrottlerConstraint.h>

#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerSemaphoreConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<ThrottlerConstraint>("bandwidth_limit");
}

}
