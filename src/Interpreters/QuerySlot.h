#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>


namespace DB
{

class QuerySlot: public boost::noncopyable
{
private:
    class Request;
    friend class Request; // for grant() and failed()

    // Represents a resource request for a cpu slot for a single thread
    class Request final : public ResourceRequest
    {
    public:
        Request() = default;
        ~Request() override = default;

        /// Callback to trigger resource consumption.
        void execute() override;

        /// Callback to trigger an error in case if resource is unavailable.
        void failed(const std::exception_ptr & ptr) override;

        QuerySlot * slot = nullptr;
    };

public:
    /// Blocks until a query slot is acquired
    /// May throw if time limit exceeded
    /// NOTE: mutex is shared with ProcessList to avoid blocking whole ProcessList while we wait for a slot
    QuerySlot(ResourceLink link_);

    ~QuerySlot();

private:
    // Resource request failed
    void failed(const std::exception_ptr & ptr);

    // Enqueue resource request if necessary
    void grant();

    ResourceLink link;
    Request request;

    std::mutex mutex;
    std::condition_variable cv;
    bool granted = false;
    std::exception_ptr exception;
    std::optional<CurrentMetrics::Increment> acquired_slot_increment;
};

using QuerySlotPtr = std::unique_ptr<QuerySlot>;

}
