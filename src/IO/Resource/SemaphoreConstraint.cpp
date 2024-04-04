#include <IO/Resource/SemaphoreConstraint.h>

#include <IO/SchedulerNodeFactory.h>

namespace DB
{

void registerSemaphoreConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<SemaphoreConstraint>("inflight_limit");
}

}
