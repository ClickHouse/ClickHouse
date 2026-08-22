#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ISchedulerConstraint.h>

#include <Common/Exception.h>

#include <ranges>

namespace DB
{

void ResourceRequest::finish()
{
    // Iterate over constraints in reverse order
    for (ISchedulerConstraint * constraint : std::ranges::reverse_view(constraints))
    {
        if (constraint)
            constraint->finishRequest(this);
    }
}

bool ResourceRequest::addConstraint(ISchedulerConstraint * new_constraint)
{
    for (auto & constraint : constraints)
    {
        if (!constraint)
        {
            constraint = new_constraint;
            return true;
        }
    }
    return false;
}

}
