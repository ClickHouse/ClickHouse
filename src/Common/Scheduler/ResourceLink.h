#pragma once

#include <base/types.h>

namespace DB
{

class ISchedulerQueue;
class IAllocationQueue;
class ResourceSchedulingContext;
using ResourceCost = Int64;

/*
 * Everything required for resource consumption. Connection to a specific resource queue.
 */
struct ResourceLink
{
    /// Queue to enqueue resource requests to. Only one of the two fields is set.
    ISchedulerQueue * queue = nullptr; // queue for time-shared resources (CPU, network, etc)
    IAllocationQueue * allocation_queue = nullptr; // queue for space-shared resources (memory, disk, etc)

    /// Per-query scheduling context, stamped by the classifier that produced this link (non-owning;
    /// the classifier owns it for the query's lifetime). Requests tagged with this link carry it, so
    /// the query-aware schedulers can always assume a context. Null for links not produced by a
    /// classifier (internal/test `getLink()`), which never tag query requests.
    ResourceSchedulingContext * scheduling_context = nullptr;

    /// Identity is the resource target only; the context is derived from the same classifier as the
    /// queue, so it does not participate in comparison.
    bool operator==(const ResourceLink & rhs) const
    {
        return queue == rhs.queue && allocation_queue == rhs.allocation_queue;
    }

    explicit operator bool() const
    {
        return queue != nullptr || allocation_queue != nullptr;
    }

    void reset()
    {
        queue = nullptr;
        allocation_queue = nullptr;
        scheduling_context = nullptr;
    }
};

/*
 * Everything required for IO scheduling.
 * Note that raw pointer are stored inside, so make sure that `ClassifierPtr` that produced
 * resource links will outlive them. Usually classifier is stored in query `Context`.
 */
struct IOSchedulingSettings
{
    ResourceLink read_resource_link;
    ResourceLink write_resource_link;

    bool operator==(const IOSchedulingSettings &) const = default;
    explicit operator bool() const { return read_resource_link && write_resource_link; }
};

}
