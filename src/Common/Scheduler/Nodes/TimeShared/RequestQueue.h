#pragma once

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>
#include <Common/Scheduler/CostUnit.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <Common/BitHelpers.h>

#include <boost/intrusive/set.hpp>

#include <algorithm>
#include <atomic>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SCHEDULER_NODE;
    extern const int SERVER_OVERLOADED;
    extern const int BAD_ARGUMENTS;
}

/// Which scheduling algorithm a `RequestQueue` leaf runs. Selected by the WORKLOAD setting
/// `scheduler`. `fifo` reproduces the historical first-come-first-served behaviour and is the default.
enum class SchedulerAlgorithm
{
    Fifo, /// First-come-first-served
    Fair, /// Weighted fair queueing (SFQ) with per-query weight lowering
    Las, /// Least-Attained-Service, MLFQ-bucketed (favours short queries; can starve long ones)
    Priority, /// Strict priority by the query's `priority` setting (lower value first; can starve)
};

inline SchedulerAlgorithm parseSchedulerAlgorithm(const String & name)
{
    if (name.empty() || name == "fifo")
        return SchedulerAlgorithm::Fifo;
    if (name == "fair")
        return SchedulerAlgorithm::Fair;
    if (name == "las")
        return SchedulerAlgorithm::Las;
    if (name == "priority")
        return SchedulerAlgorithm::Priority;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown workload scheduler '{}' (expected 'fifo', 'fair', 'las' or 'priority')", name);
}

/// Pluggable ordering strategy owned by a `RequestQueue`. It owns only the container of pending
/// requests and the ordering; the enclosing `RequestQueue` owns all the cross-cutting concerns
/// (mutex, budget, `max_waiting_queries`, counters, activation). All methods are called under the
/// `RequestQueue` mutex.
class ISchedulingAlgorithm
{
public:
    virtual ~ISchedulingAlgorithm() = default;

    /// Insert a pending request (computing its ordering key from the query context if needed).
    virtual void push(ResourceRequest * request) = 0;

    /// Remove and return the next request to serve, or nullptr if empty.
    virtual ResourceRequest * pop() = 0;

    /// Remove a specific request if it is enqueued in this algorithm. Returns true iff removed.
    virtual bool erase(ResourceRequest * request) = 0;

    /// Remove and return the least-preferred request (served last), or nullptr if empty.
    /// Used to trim to `max_waiting_queries`.
    virtual ResourceRequest * popWorst() = 0;

    /// Drain all pending requests into `out` (for purge and for the scheduler-swap hook).
    virtual void pullAll(std::vector<ResourceRequest *> & out) = 0;

    virtual bool empty() const = 0;
};

/// `fifo` — first-come-first-served. Byte-for-byte the historical leaf ordering.
class FifoAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override { requests.push_back(*request); }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        ResourceRequest * request = &requests.front();
        requests.pop_front();
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->enqueued_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        ResourceRequest * request = &requests.back();
        requests.pop_back();
        return request;
    }

    void pullAll(std::vector<ResourceRequest *> & out) override
    {
        while (!requests.empty())
        {
            out.push_back(&requests.front());
            requests.pop_front();
        }
    }

    bool empty() const override { return requests.empty(); }

private:
    ResourceRequest::EnqueuedList requests;
};

/// `fair` — weighted fair queueing (Start-time Fair Queueing). Requests are ordered by a virtual
/// runtime key; a newly active query starts at the system virtual time so it is not penalised for
/// idle periods (no starvation). A query's effective weight is lowered once it crosses an age or
/// attained-service threshold (see the query settings), which biases the fair shares toward
/// shorter/newer queries.
class FairAlgorithm final : public ISchedulingAlgorithm
{
public:
    explicit FairAlgorithm(CostUnit unit_)
        : unit(unit_)
    {
    }

    void push(ResourceRequest * request) override
    {
        auto & state = *request->scheduling.state;
        double effective_weight = updateEffectiveWeight(*request->scheduling.context, state);
        // Charge the declared cost plus any pending real-vs-estimate correction — folded here from
        // fair's own accumulator, independent of attained_cost; never negative, so vruntime only
        // moves forward.
        ResourceCost charge = state.drainVruntimeCorrection(request->scheduling.cost);
        double vstart = std::max(system_vruntime, state.vruntime);
        state.vruntime = vstart + static_cast<double>(charge) / effective_weight;
        request->scheduling.key = {vstart, next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        // System virtual time advances to the start tag of the served request (monotonic).
        // attained_cost + last_activity_ns are charged generically in RequestQueue::dequeueRequest.
        system_vruntime = std::max(system_vruntime, request->scheduling.key.first);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(std::vector<ResourceRequest *> & out) override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            out.push_back(&*it);
            requests.erase(it);
        }
    }

    bool empty() const override { return requests.empty(); }

private:
    /// Fair effective weight: the query's `weight`, lowered once by `weight_lowering_factor` the
    /// first time it crosses any configured threshold (thresholds do not combine — the first to trip
    /// applies the full lowering). Lowering is a one-way latch (`weight_lowered`): the cached
    /// `effective_weight` is reused afterwards, so the threshold checks run only until the crossing.
    /// Called at push(), so lowering biases the query's subsequent requests — a request already keyed
    /// keeps its weight; the bounded lag is fine for a single-step bias (unlike `las`, which re-keys
    /// on pop()).
    double updateEffectiveWeight(const ResourceSchedulingContext & ctx, ResourceQueryState & state) const
    {
        if (!state.weight_lowered)
        {
            if (weightLoweringThresholdCrossed(ctx, state))
            {
                state.weight_lowered = true;
                double weight = ctx.weight * ctx.weight_lowering_factor;
                state.effective_weight = weight > 0 ? weight : 1e-9; // guard against division by zero
            }
            else
            {
                state.effective_weight = ctx.weight > 0 ? ctx.weight : 1e-9;
            }
        }
        return state.effective_weight;
    }

    /// True once the query's real cumulative service crosses a `weight_lowering_*` threshold.
    /// `attained_cost` already tracks real service (finish folds its correction straight in). For CPU
    /// it is granted service and leads spent CPU by at most one quantum.
    bool weightLoweringThresholdCrossed(const ResourceSchedulingContext & ctx, const ResourceQueryState & state) const
    {
        if (ctx.weight_lowering_age_seconds > 0)
        {
            UInt64 now = clock_gettime_ns();
            double age_seconds = now > ctx.start_ns ? static_cast<double>(now - ctx.start_ns) / 1e9 : 0.0;
            if (age_seconds >= ctx.weight_lowering_age_seconds)
                return true;
        }
        const Int64 attained_service = state.attained_cost.load(std::memory_order_relaxed);
        if (unit == CostUnit::CPUNanosecond && ctx.weight_lowering_cpu_seconds > 0
            && static_cast<double>(attained_service) / 1e9 >= ctx.weight_lowering_cpu_seconds)
            return true;
        if (unit == CostUnit::IOByte && ctx.weight_lowering_io_bytes > 0
            && static_cast<double>(attained_service) >= ctx.weight_lowering_io_bytes)
            return true;
        return false;
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const CostUnit unit;
    double system_vruntime = 0.0;
    UInt64 next_seq = 0;
    Set requests;
};

/// `las` — practical Least-Attained-Service, MLFQ-bucketed. Serves the query that has attained the
/// least service in this resource first (best mean latency under heavy-tailed query sizes),
/// coarsened into geometric levels (`level = floor(log2(1 + attained/base))`) so a query drops one
/// level each time its attained service doubles — bounding reordering churn. Lowest level served
/// first, FIFO within a level. Pure: there is no starvation guard, so a long-running query can be
/// starved by a continuous stream of short ones — use `fair` when a no-starvation guarantee is
/// needed.
class LasAlgorithm final : public ISchedulingAlgorithm
{
public:
    explicit LasAlgorithm(CostUnit unit_)
        : base(baseQuantum(unit_))
    {
    }

    void push(ResourceRequest * request) override
    {
        Int64 attained = request->scheduling.state->attained_cost.load(std::memory_order_relaxed);
        request->scheduling.key = {levelOf(attained), next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        // Lazy re-keying: a request's stored level (levelOf(attained) at enqueue) only goes stale
        // too LOW as the query's other requests accrue service. On pop, if the front request's true
        // level (from current attained) exceeds its key, re-key and defer it; otherwise serve it.
        // Restores bucketed LAS when a query has several requests queued at once (IO), a no-op with
        // one (CPU). Bounded to ~log2(total_service) re-keys per request; the FIFO tie-break is kept.
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            // `attained_cost` already tracks real service (finish folds its correction straight in),
            // so re-key from its current value: if the query's accrued service has pushed the true
            // level past this request's stored key, re-key and defer it; otherwise serve it.
            UInt32 real_level = levelOf(request->scheduling.state->attained_cost.load(std::memory_order_relaxed));
            if (real_level > request->scheduling.key.first)
            {
                requests.erase(it);
                request->scheduling.key.first = real_level;
                requests.insert(*request);
                continue;
            }
            requests.erase(it);
            // attained_cost + last_activity_ns are charged generically in RequestQueue::dequeueRequest.
            return request;
        }
        return nullptr;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(std::vector<ResourceRequest *> & out) override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            out.push_back(&*it);
            requests.erase(it);
        }
    }

    bool empty() const override { return requests.empty(); }

private:
    /// Internal per-resource quantum used as the bucketing unit (not exposed as a SQL setting).
    static ResourceCost baseQuantum(CostUnit unit_)
    {
        switch (unit_)
        {
            case CostUnit::CPUNanosecond: return 10'000'000; // ~10 ms CPU lease quantum
            case CostUnit::IOByte: return 1'048'576; // 1 MiB
            case CostUnit::QuerySlot: return 1;
            case CostUnit::MemoryByte: return 1;
        }
        return 1;
    }

    UInt32 levelOf(Int64 attained) const
    {
        if (attained <= 0)
            return 0;
        // MLFQ level = floor(log2(1 + attained/base)). Since 1 + attained/base = (attained + base)/base
        // and floor(log2(floor(r))) == floor(log2(r)) for r >= 1, the level is the index of the highest
        // set bit of the integer quotient — a single bit-scan, avoiding FP log2/floor on the hot path.
        UInt64 quotient = (static_cast<UInt64>(attained) + static_cast<UInt64>(base)) / static_cast<UInt64>(base);
        return bitScanReverse(quotient);
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling.key < rhs.scheduling.key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const ResourceCost base;
    UInt64 next_seq = 0;
    Set requests;
};

/// `priority` — strict priority by the query's `workload_priority` setting. Lower value = higher
/// precedence, served first; the default `0` is the neutral baseline, a negative value raises the
/// query above it and a positive value lowers it. Ties (equal priority) fall back to FIFO by
/// arrival. Like any strict-priority scheme it can starve low-priority queries — `fair` is the
/// non-starving alternative.
class PriorityAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        // Order by the query's `workload_priority` (Int64): lower value = higher precedence, so a
        // negative value sorts ahead of the default `0` and a positive value behind it. Ties break
        // FIFO by arrival sequence.
        request->scheduling.priority = Priority{request->scheduling.context->priority};
        request->scheduling.key = {0.0, next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        if (requests.empty())
            return nullptr;
        auto it = requests.begin();
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    bool erase(ResourceRequest * request) override
    {
        if (!request->scheduling_hook.is_linked())
            return false;
        requests.erase(requests.iterator_to(*request));
        return true;
    }

    ResourceRequest * popWorst() override
    {
        if (requests.empty())
            return nullptr;
        auto it = std::prev(requests.end());
        ResourceRequest * request = &*it;
        requests.erase(it);
        return request;
    }

    void pullAll(std::vector<ResourceRequest *> & out) override
    {
        while (!requests.empty())
        {
            auto it = requests.begin();
            out.push_back(&*it);
            requests.erase(it);
        }
    }

    bool empty() const override { return requests.empty(); }

private:
    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            // Integer priority first (exact, lower value = higher priority), then the sequence for FIFO.
            if (lhs.scheduling.priority.value != rhs.scheduling.priority.value)
                return lhs.scheduling.priority.value < rhs.scheduling.priority.value;
            return lhs.scheduling.key.second < rhs.scheduling.key.second;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    UInt64 next_seq = 0;
    Set requests;
};

/*
 * Time-shared scheduler leaf that runs one of several pluggable scheduling algorithms, chosen by
 * the workload setting `scheduler` (default `fifo`). It is the time-shared
 * workload leaf. The leaf owns the cross-cutting concerns (mutex, budget via `ISchedulerQueue`,
 * `max_waiting_queries`, counters, activation) and delegates ordering to an `ISchedulingAlgorithm`.
 *
 * `setScheduler()` swaps the algorithm in place (pulling all pending requests from the old one and
 * pushing them into the new one) so a `CREATE OR REPLACE WORKLOAD` that changes `scheduler` neither
 * rebuilds the hierarchy nor invalidates the `ResourceLink` cached by classifiers, and loses no
 * pending requests.
 */
class RequestQueue final : public ISchedulerQueue
{
    static constexpr Int64 default_max_queued = std::numeric_limits<Int64>::max();

public:
    explicit RequestQueue(
        EventQueue & event_queue_,
        const SchedulerNodeInfo & info_ = {},
        SchedulerAlgorithm algorithm_ = SchedulerAlgorithm::Fifo,
        CostUnit unit_ = CostUnit::IOByte,
        Int64 max_queued_ = default_max_queued)
        : ISchedulerQueue(event_queue_, info_)
        , unit(unit_)
        , max_queued(max_queued_)
        , algorithm(algorithm_)
    {
        algo = makeAlgorithm(algorithm_, unit_);
    }

    ~RequestQueue() override
    {
        purgeQueue();
    }

    // Distinct type (parallels AllocationQueue's "allocation_queue"); the node's basename in the
    // hierarchy is "queue" (see WorkloadNodeTraits::makeQueue), which unifies the path naming.
    std::string_view getTypeName() const override { return "request_queue"; }

    void enqueueRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue is about to be destructed");

        if (max_queued >= 0 && total_requests >= static_cast<size_t>(max_queued))
        {
            rejected_requests++;
            rejected_cost += request->cost;
            throw Exception(ErrorCodes::SERVER_OVERLOADED,
                "Workload limit `max_waiting_queries` has been reached: {} of {}", total_requests, max_queued);
        }

        // Tag the accounting this leaf's algorithm needs, so dequeueRequest()/finish() act without
        // consulting the leaf: `fair`/`las` accrue attained service; only `fair` also corrects vruntime.
        request->scheduling.tracks_attained = algorithm == SchedulerAlgorithm::Fair || algorithm == SchedulerAlgorithm::Las;
        request->scheduling.tracks_vruntime = algorithm == SchedulerAlgorithm::Fair;
        algo->push(request);
        queue_cost += request->cost;
        bool was_empty = total_requests == 0;
        total_requests++;
        SCHED_DBG("{} -- enqueue(cost={}, queued={})", basename, request->cost, total_requests);
        if (was_empty)
            scheduleActivation();
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        std::lock_guard lock(mutex);
        ResourceRequest * request = algo->pop();
        if (!request)
            return {nullptr, false};
        // Charge attained service generically (algorithm-independent): the declared cost is the
        // estimate now that the request is served; `finish()` corrects it to real cost later.
        if (request->scheduling.tracks_attained)
        {
            request->scheduling.state->attained_cost.fetch_add(request->scheduling.cost, std::memory_order_relaxed);
            request->scheduling.state->last_activity_ns = clock_gettime_ns();
        }
        queue_cost -= request->cost;
        total_requests--;
        if (total_requests == 0)
        {
            busy_periods++;
            cancelActivation();
        }
        incrementDequeued(request->cost);
        SCHED_DBG("{} -- dequeue(cost={}, queued={})", getPath(), request->cost, total_requests);
        return {request, total_requests > 0};
    }

    bool cancelRequest(ResourceRequest * request) override
    {
        std::lock_guard lock(mutex);
        if (is_not_usable)
            return false; // Any request should already be failed or executed
        if (!algo->erase(request))
            return false;
        queue_cost -= request->cost;
        total_requests--;
        canceled_requests++;
        canceled_cost += request->cost;
        if (total_requests == 0)
        {
            busy_periods++;
            cancelActivation();
        }
        return true;
    }

    void purgeQueue() override
    {
        // Collect requests to fail while holding the lock, but call failed() outside the lock
        // to avoid potential deadlock with CPULeaseAllocation::mutex (lock order inversion).
        std::vector<ResourceRequest *> requests_to_fail;
        {
            std::lock_guard lock(mutex);
            is_not_usable = true;
            algo->pullAll(requests_to_fail);
            queue_cost = 0;
            total_requests = 0;
            cancelActivation();
        }
        auto exception = std::make_exception_ptr(
            Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Scheduler queue with resource request is about to be destructed"));
        for (ResourceRequest * request : requests_to_fail)
            request->failed(exception);
    }

    void updateQueueLimit(Int64 value) override
    {
        std::vector<ResourceRequest *> requests_to_fail;
        {
            std::lock_guard lock(mutex);
            // `0` means "reject every waiting request" — a valid limit, as at construction and in
            // AllocationQueue; only a negative value is invalid.
            if (value < 0)
                throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Queue limit must not be negative, got: {}", value);
            max_queued = value;
            while (total_requests > static_cast<size_t>(max_queued))
            {
                ResourceRequest * request = algo->popWorst();
                chassert(request);
                queue_cost -= request->cost;
                total_requests--;
                rejected_requests++;
                rejected_cost += request->cost;
                requests_to_fail.push_back(request);
            }
            if (total_requests == 0)
            {
                busy_periods++;
                cancelActivation();
            }
        }
        auto exception = std::make_exception_ptr(
            Exception(ErrorCodes::SERVER_OVERLOADED, "Workload limit `max_waiting_queries` has been reached"));
        for (ResourceRequest * request : requests_to_fail)
            request->failed(exception);
    }

    /// Swap the scheduling algorithm in place, migrating all pending requests (swap hook called by
    /// `WorkloadResourceManager` when the workload `scheduler` setting changes). No effect on the
    /// node identity, `ResourceLink`, activation state, or the pending-request count.
    void setScheduler(SchedulerAlgorithm new_algorithm)
    {
        std::lock_guard lock(mutex);
        if (new_algorithm == algorithm)
            return;
        std::vector<ResourceRequest *> pending;
        algo->pullAll(pending);
        algo = makeAlgorithm(new_algorithm, unit);
        algorithm = new_algorithm;
        // Migrate the backlog to the new algorithm. When switching to `fair`, reset each migrated
        // query's vruntime: the fresh instance restarts system virtual time at 0, so the stale
        // projection would otherwise be double-counted. attained_cost is real accrued service, kept.
        if (new_algorithm == SchedulerAlgorithm::Fair)
            for (ResourceRequest * request : pending)
                request->scheduling.state->vruntime = 0.0;
        for (ResourceRequest * request : pending)
            algo->push(request);
    }

    SchedulerAlgorithm getScheduler() const
    {
        std::lock_guard lock(mutex);
        return algorithm;
    }

    bool isActive() override
    {
        std::lock_guard lock(mutex);
        return total_requests > 0;
    }

    size_t activeChildren() override { return 0; }

    void activateChild(ITimeSharedNode &) override
    {
        chassert(false); // queue cannot have children
    }

    void attachChild(const SchedulerNodePtr &) override
    {
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Cannot add child to leaf scheduler queue: {}", getPath());
    }

    void removeChild(ISchedulerNode *) override {}

    ISchedulerNode * getChild(const String &) override { return nullptr; }

    std::pair<UInt64, Int64> getQueueLengthAndCost() override
    {
        std::lock_guard lock(mutex);
        return {total_requests, queue_cost};
    }

private:
    static std::unique_ptr<ISchedulingAlgorithm> makeAlgorithm(SchedulerAlgorithm algorithm_, CostUnit unit_)
    {
        switch (algorithm_)
        {
            case SchedulerAlgorithm::Fifo:
                return std::make_unique<FifoAlgorithm>();
            case SchedulerAlgorithm::Fair:
                return std::make_unique<FairAlgorithm>(unit_);
            case SchedulerAlgorithm::Las:
                return std::make_unique<LasAlgorithm>(unit_);
            case SchedulerAlgorithm::Priority:
                return std::make_unique<PriorityAlgorithm>();
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected scheduler algorithm");
    }

    mutable std::mutex mutex;
    const CostUnit unit;
    Int64 max_queued;
    SchedulerAlgorithm algorithm;
    std::unique_ptr<ISchedulingAlgorithm> algo;
    ResourceCost queue_cost = 0;
    size_t total_requests = 0;
    bool is_not_usable = false;
};

}
