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
#include <Common/Logger.h>

#include <condition_variable>
#include <mutex>
#include <chrono>

namespace DB
{

struct CPULeaseSettings
{
    static constexpr ResourceCost default_quantum_ns = 10'000'000;
    static constexpr ResourceCost default_report_ns = default_quantum_ns / 10;
    static constexpr std::chrono::milliseconds default_preemption_timeout = std::chrono::milliseconds(1000);

    /// Estimated cost for requests, consumption limit without renewal
    ResourceCost quantum_ns = default_quantum_ns;

    /// Minimum CPU consumption to report
    ResourceCost report_ns = default_report_ns;

    /// Timeout after which preempted thread should exit
    std::chrono::milliseconds preemption_timeout = default_preemption_timeout;

    /// For debugging purposes, not used in production
    String workload;
};

class CPULeaseAllocation;
using CPULeaseAllocationPtr = std::shared_ptr<CPULeaseAllocation>;

/// TODO(serxa): add description and implementation details
class CPULeaseAllocation final : public ISlotAllocation
{
private:
    /// Controls a cpu slot of one specific thread.
    class Lease;
    friend class Lease; // for renew() and release()
    class Lease final : public ISlotLease
    {
    public:
        explicit Lease(CPULeaseAllocationPtr && parent_, size_t slot_id_);
        ~Lease() override;
        void startConsumption() override;
        bool renew() override;

    private:
        friend class CPULeaseAllocation;
        CPULeaseAllocationPtr parent; // Hold allocation to enforce destruction order
        UInt64 last_report_ns = 0; // Last time when the slot was renewed or started
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
        bool is_master_slot = false; /// (true) master or (false) worker slot
        bool is_noncompeting = false;
    };

public:
    CPULeaseAllocation(
        SlotCount max_threads_,
        ResourceLink master_link_,
        ResourceLink worker_link_,
        CPULeaseSettings settings = {});
    ~CPULeaseAllocation() override;

    /// Take one already granted slot if available. Never blocks or waits for slots.
    /// Should be used before spawning worker threads for a query.
    [[nodiscard]] AcquiredSlotPtr tryAcquire() override;

    /// It either (a) takes already granted slot or (b) burrows slot (which we expect later to be granted).
    /// It never blocks or waits for slots. Should be used to acquire the master thread for a query.
    [[nodiscard]] AcquiredSlotPtr acquire() override;

    // For tests only. Returns true iff resource request is enqueued in the scheduler
    bool isRequesting() const override;

private:
    /// Helper to make a lease
    AcquiredSlotPtr acquireImpl(std::unique_lock<std::mutex> & lock);

    /// Registers an additional leased thread and returns its thread_num
    size_t upscale();

    /// Unregisters specified thread from leased set
    void downscale(size_t thread_num);

    /// Preempted thread set management
    void setPreempted(size_t thread_num);
    void resetPreempted(size_t thread_num);

    /// Resource request failed.
    void failed(const std::exception_ptr & ptr);

    /// Grant a slot and enqueue another resource request if necessary.
    void grant();
    void grantImpl(std::unique_lock<std::mutex> & lock);

    /// Report real CPU consumption by a thread.
    /// Returns true if renewal has been successful,
    /// otherwise the thread should be stopped (timeout during lease renewal).
    bool renew(Lease & lease);

    /// Preemption. Block CPU consumption: waits for a slot to be granted to this thread.
    bool waitForGrant(std::unique_lock<std::mutex> & lock, size_t thread_num);

    /// Accounts consumed resource
    void consume(std::unique_lock<std::mutex> & lock, ResourceCost delta_ns);

    /// Enqueue a resource request to the scheduler if necessary.
    void schedule(std::unique_lock<std::mutex> & lock);

    /// Thread stops and completely releases its lease.
    void release(Lease & lease);

    /// Configuration
    const SlotCount max_threads; /// Max number of threads (and allocated slots)
    const ResourceLink master_link; /// Resource link to use for master thread resource requests
    const ResourceLink worker_link; /// Resource link to use for worker threads resource requests
    const CPULeaseSettings settings;
    LoggerPtr log;

    /// Protects all the fields below
    mutable std::mutex mutex;

    /// Concurrency control (for interaction with consuming threads)
    /// Every thread could be considered as a finite state machine w/states:
    ///  * released: lease object was not created or was destructed, has no CPU slot
    ///  * running: lease object owns a CPU slot
    ///  * preempted: lease object does not own a CPU slot
    /// Possible transitions:
    ///  * released -> running: initial acquire() or tryAcquire() call
    ///    - thread starts execution
    ///  * running -> preempted: acquired slot was taken away during renew() and lease waits for another granted slot
    ///    - thread execution is blocked
    ///  * preempted -> running: newly granted slot was provided to lease
    ///    - thread execution is unblocked
    ///  * preempted -> released: timeout during renew() preemption wait - lease expired
    ///    - renew() returns false and thread should stop itself
    ///  * running -> released: lease destruction and release() of its acquired slot
    ///    - thread execution stop voluntary (query is done/aborted/canceled)
    struct Threads
    {
        explicit Threads(size_t max_threads_)
            : leased(max_threads_)
            , preempted(max_threads_)
            , wake(max_threads_)
        {}
        boost::dynamic_bitset<> leased; /// Thread lease object status bitmask (0=released; 1=preempted|running)
        boost::dynamic_bitset<> preempted; /// Preempted threads bitmask (0=running|released; 1=preempted)
        std::vector<std::condition_variable> wake; /// To wake specific preempted thread

        // For optimization (could be computed based on leased and preempted fields)
        size_t running_count = 0; /// Number of currently running threads (leased & !preempted)
        size_t last_running = boost::dynamic_bitset<>::npos; /// Highest thread num of a running threads
    } threads;

    /// Resource accounting
    std::atomic_bool acquirable{false}; // Tracks `(granted > 0 || exception) && !shutdown` value that could be read w/o locking mutex
    SlotCount allocated = 0; /// Current number of allocated (granted and acquired) slots
    Int64 granted = 0; /// Allocated but not acquired slots (might be negative if acquired more than allocated)
    ResourceCost consumed_ns = 0; /// Real consumption accumulated from renew() calls
    ResourceCost requested_ns = 0; /// Consumption requested from the scheduler (requested <= consumed + quantum)

    /// Scheduling control (for interaction with resource scheduler)
    using Requests = std::vector<Request>;
    Requests requests; /// Circular buffer of requests per every slot
    Requests::iterator head; /// Next request to be enqueued
    Requests::iterator tail; /// Next request to be finished
    bool enqueued = false; /// True if the next request is already enqueued to the scheduler
    bool request_master_slot = true; /// The next request should use (true) master_link or (false) worker_link
    std::exception_ptr exception; /// Exception from the scheduler
    bool shutdown = false; /// True if the destructor is called and we should stop scheduling
    std::condition_variable shutdown_cv; /// Used to notify waiting destructor

    /// Introspection
    CurrentMetrics::Increment acquired_increment;
    CurrentMetrics::Increment scheduled_increment;
    std::optional<ProfileEvents::Timer> wait_timer;
};

}
