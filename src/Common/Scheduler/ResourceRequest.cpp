#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ISchedulerConstraint.h>

namespace DB
{

void ResourceRequest::finish()
{
    if (constraint)
        constraint->finishRequest(this);
}

}
