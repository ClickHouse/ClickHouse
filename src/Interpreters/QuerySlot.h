#pragma once

#include <base/types.h>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceAllocation.h>
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
// A query slot is a space-shared allocation of size one: it is admitted (possibly after waiting in the queue),
// held while the query runs and released on destruction (when the query execution finishes). Query slots are never evicted.
// Specified link should point to an allocation queue of some workload within the resource created with:
//   CREATE RESOURCE query (QUERY)
class QuerySlot final : public ResourceAllocation
{
public:
    /// Blocks until a query slot is acquired or the request fails. `admission_deadline_` is an absolute
    /// steady_clock deadline shared with the query's memory reservation so the whole admission phase is
    /// bounded by one budget; on expiry the still-pending allocation is canceled and a
    /// `QUERY_SLOT_ACQUISITION_TIMEOUT` exception is thrown. `time_point::max()` means no timeout.
    explicit QuerySlot(
        ResourceLink link_,
        std::chrono::steady_clock::time_point admission_deadline_ = std::chrono::steady_clock::time_point::max(),
        const String & id_ = {});
    ~QuerySlot() override;

private:
    // ResourceAllocation callbacks. They are called from the scheduler thread under the queue mutex.
    void increaseApproved(const IncreaseRequest & increase) override;
    void decreaseApproved(const DecreaseRequest & decrease) override;
    void allocationFailed(const std::exception_ptr & reason) override;
    void killAllocation(const std::exception_ptr & reason) override;
    void spillAllocation(ResourceCost additional_bytes) override;

    /// Releases the slot (or cancels the pending allocation) and waits until the scheduler is done with it.
    void detachFromQueue();

    std::mutex mutex;
    std::condition_variable cv;
    bool granted = false; /// The slot has been admitted
    bool removed = false; /// The allocation is no longer known to the scheduler
    std::exception_ptr fail_reason;
    std::optional<CurrentMetrics::Increment> acquired_slot_increment;
};

using QuerySlotPtr = std::unique_ptr<QuerySlot>;

}
