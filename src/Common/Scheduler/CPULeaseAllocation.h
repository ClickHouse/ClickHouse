#pragma once

#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/pool/pool_alloc.hpp>

#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/ISlotControl.h>

#include <condition_variable>
#include <mutex>

namespace DB
{

class CPULeaseAllocation;
using CPULeaseAllocationPtr = std::shared_ptr<CPULeaseAllocation>;

/// TODO(serxa): add description and implementation details
class CPULeaseAllocation final : public ISlotAllocation
{
private:
    /// Controls a cpu slot of one specific thread.
    class Lease;
    friend class Lease; // for renew()
    class Lease final : public ISlotLease
    {
    public:
        explicit Lease(CPULeaseAllocationPtr && parent_, size_t thread_num_);
        ~Lease() override;
        void startConsumption() override;
        bool renew() override;

    private:
        friend class CPULeaseAllocation;
        CPULeaseAllocationPtr parent; // Hold allocation to enforce destruction order
        size_t thread_num; // Thread number that acquired the slot
        UInt64 last_report_ns = 0; // Last time when the slot was renewed or started
        CurrentMetrics::Increment acquired_slot_increment;
    };

    /// Represents a resource request for a cpu slot.
    /// Request is send to the scheduler every time lease requires to be renewed.
    /// Only one request may be enqueued at a time.
    /// Multiple requests may be in consumption state.
    class Request;
    friend class Request; // for failed() and grant()
    class Request final : public ResourceRequest
    {
    public:
        Request() = default;
        ~Request() override = default;

        /// Callback to trigger resource consumption.
        void execute() override
        {
            chassert(lease);
            lease->grant();
        }

        /// Callback to trigger an error in case if resource is unavailable.
        void failed(const std::exception_ptr & ptr) override
        {
            chassert(lease);
            lease->failed(ptr);
        }

        CPULeaseAllocation * lease = nullptr;
        ResourceCost max_consumed = 0; /// Maximum consumption value for this request before it should be finished
    };

public:
    CPULeaseAllocation(SlotCount max_threads_, ResourceLink cpu_link_);
    ~CPULeaseAllocation() override;

    /// Take one already granted slot if available. Never blocks or waits for slots.
    /// Should be used before spawning worker threads for a query.
    [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

    /// It either (a) takes already granted slot or (b) burrows slot (which we expect later to be granted).
    /// It never blocks or waits for slots. Should be used to acquire the master thread for a query.
    [[nodiscard]] AcquiredSlotPtr acquire() override;

private:
    /// Resource request failed.
    void failed(const std::exception_ptr & ptr);

    /// Grant a slot and enqueue another resource request if necessary.
    void grant();

    /// Report real CPU consumption by a thread.
    /// Returns true if renewal has been successful,
    /// otherwise the thread should be stopped (timeout during lease renewal).
    bool renew(Lease & lease);

    /// Accounts consumed resource
    void consume(std::unique_lock<std::mutex> & lock, ResourceCost delta_ns);

    /// Enqueue a resource request to the scheduler if necessary.
    void schedule(std::unique_lock<std::mutex> & lock);

    /// Wake renewing thread waiting for a granted slot.
    /// Returns true if wake was successful or false if there is no thread to wake.
    bool resumePreemptedThread(std::unique_lock<std::mutex> & lock);

    /// Thread stops and completely releases its lease.
    void release(Lease & lease);

    /// Configuration
    static const ResourceCost quantum = 10'000'000; /// Estimated cost for requests, consumption limit without renewal
    static const ResourceCost report_quantum = quantum / 10; // Mimimum CPU consumption to report
    const SlotCount max_threads; /// Max number of threads (and allocated slots)
    const ResourceLink cpu_link; /// Resource link to use for resource requests

    /// Protects all the fields below
    mutable std::mutex mutex;

    /// Concurrency control (for interaction with consuming threads)
    ResourceCost consumed = 0; /// Real consumption accumulated from renew() calls
    ResourceCost requested = 0; /// Consumption requested from the scheduler (requested <= consumed + quantum)
    Int64 granted = 0; /// Allocated slots left to acquire (might be negative if acquired more than allocated)
    boost::dynamic_bitset<> acquired_threads; /// Acquired threads bitmask (0=released|not-acquired; 1=preempted|running)
    boost::dynamic_bitset<> preempted_threads; /// Preempted threads bitmask (0=running|released|not-acquired; 1=preempted)
    std::vector<std::condition_variable> wake_threads; /// To wake specific preempted thread

    /// Scheduling control (for interaction with resource scheduler)
    std::exception_ptr exception;
    SlotCount cur_slots = 0; /// Current number of allocated (granted and acquired) slots
    using Requests = std::vector<Request>;
    Requests requests; /// Circular buffer of requests per every slot
    Requests::iterator head; /// Next request to be enqueued
    Requests::iterator tail; /// Next request to be finished
    bool enqueued = false; /// True if the next request is already enqueued to the scheduler
    bool shutdown = false; /// True if the destructor is called and we should stop scheduling
    std::condition_variable shutdown_cv; /// Used to notify waiting destructor

    /// Introspection
    std::optional<CurrentMetrics::Increment> scheduled_slot_increment;
    std::optional<ProfileEvents::Timer> wait_timer;
};

}
