#pragma once

#include <IO/ISchedulerNode.h>

#include <memory>


namespace DB
{

/*
 * Queue for pending requests for specific resource, leaf of hierarchy.
 */
class ISchedulerQueue : public ISchedulerNode
{
public:
    ISchedulerQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    /// Enqueue new request to be executed using underlying resource.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void enqueueRequest(ResourceRequest * request) = 0;
};

}
