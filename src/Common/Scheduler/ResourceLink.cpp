#include <Common/Scheduler/ResourceLink.h>

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>

namespace DB
{

/// Shared fallback scheduling state for requests not stamped by a classifier — internal scheduler
/// plumbing and the test harness, which only run through `fifo`. It lets `scheduling.state` be
/// non-null everywhere, so the hot path dereferences it without a null check. The query-aware
/// algorithms (`fair`/`las`) only ever run on classifier-stamped requests, so they never read it;
/// writes to it (generic attained-service accounting) are harmless and go unread.
ResourceQueryState default_scheduling_state;

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
