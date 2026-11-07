#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>

#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <memory>
#include <optional>


namespace DB
{

// Represents a slot for a query execution. Every query that participates in workload scheduling should request one from
// the resource scheduler before query execution and hold it until query is finished.
// Specified link should point to a queue of some workload within the resource created with:
//   CREATE RESOURCE query (QUERY)
class IQuerySlot
{
public:
    virtual ~IQuerySlot() = default;
};

using QuerySlotPtr = std::unique_ptr<IQuerySlot>;

/// Common accounting for synchronous and asynchronous query admission.
class QuerySlotBase : public IQuerySlot, protected ResourceRequest, public boost::noncopyable
{
protected:
    void acquired();
    /// Call while the classifier keeping the scheduler constraints alive still exists.
    void release();

private:
    std::optional<CurrentMetrics::Increment> acquired_slot_increment;
};

class QuerySlot final : public QuerySlotBase
{
public:
    /// Blocks until a query slot is acquired or the request fails. `admission_deadline_` is an absolute
    /// steady_clock deadline shared with the query's memory reservation so the whole admission phase is
    /// bounded by one budget; on expiry the still-enqueued request is canceled and a
    /// `QUERY_SLOT_ACQUISITION_TIMEOUT` exception is thrown. `time_point::max()` means no timeout.
    explicit QuerySlot(ResourceLink link_, std::chrono::steady_clock::time_point admission_deadline_ = std::chrono::steady_clock::time_point::max());
    ~QuerySlot() override;

private:
    /// Callback to trigger resource consumption.
    void execute() override;

    /// Callback to trigger an error in case if resource is unavailable.
    void failed(const std::exception_ptr & ptr) override;

    ResourceLink link;

    std::mutex mutex;
    std::condition_variable cv;
    bool granted = false;
    std::exception_ptr exception;
};

}
