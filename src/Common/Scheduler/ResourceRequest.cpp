#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ISchedulerConstraint.h>

#include <Common/Exception.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ResourceRequest::finish()
{
    // Iterate over constraints in reverse order
    for (ISchedulerConstraint * constraint : std::ranges::reverse_view(constraints))
    {
        if (constraint)
            constraint->finishRequest(this);
    }
}

void ResourceRequest::addConstraint(ISchedulerConstraint * new_constraint)
{
    for (auto & constraint : constraints)
    {
        if (!constraint)
        {
            constraint = new_constraint;
            return;
        }
    }
    // TODO(serxa): is it possible to validate it during enqueue of resource request to avoid LOGICAL_ERRORs in the scheduler thread? possible but will not cover case of moving queue with requests inside to invalid position
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Max number of simultaneous workload constraints exceeded ({}). Remove extra constraints before using this workload.",
        ResourceMaxConstraints);
}

}
