#include <Common/Scheduler/ResourceLink.h>

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

namespace DB
{

bool ResourceLink::enqueue(ResourceRequest * request) const
{
    if (!queue)
        return false;
    // Stamp the query's scheduling pointers onto the request; the query-aware schedulers read them
    // once it is in the queue. Only meaningful for a queued request, so skip it on the no-queue path.
    request->scheduling.context = scheduling_context;
    request->scheduling.state = scheduling_state;
    queue->enqueueRequest(request);
    return true;
}

}
