#pragma once

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceSchedulingContext.h>
#include <Common/Scheduler/CostUnit.h>
#include <Common/Scheduler/Debug.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>

#include <boost/intrusive/set.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
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
/// `scheduler`. `fifo` reproduces the historical FifoQueue behaviour and is the default.
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

/// `fifo` — first-come-first-served. Byte-for-byte the historical FifoQueue ordering.
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
/// shorter/newer queries. Requests with no query context are scheduled anonymously at weight 1.
class FairAlgorithm final : public ISchedulingAlgorithm
{
public:
    FairAlgorithm(CostUnit unit_, const void * leaf_)
        : unit(unit_)
        , leaf(leaf_)
    {
    }

    void push(ResourceRequest * request) override
    {
        double vstart = system_vruntime;
        if (auto * ctx = request->scheduling_context)
        {
            auto & state = ctx->getResourceState(leaf);
            double effective_weight = effectiveWeight(*ctx, state);
            // Charge the declared cost corrected toward real consumption (see
            // ResourceState::consumeCorrectedCost). Stored on the request so pop() advances
            // attained_cost by the same corrected amount. It is never negative, so vruntime only
            // ever moves forward — a refund is realized by a smaller charge on later requests.
            request->scheduling_charge = state.consumeCorrectedCost(request->scheduling_cost);
            vstart = std::max(system_vruntime, state.vruntime);
            state.vruntime = vstart + static_cast<double>(request->scheduling_charge) / effective_weight;
        }
        request->scheduling_key = {vstart, next_seq++};
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
        system_vruntime = std::max(system_vruntime, request->scheduling_key.first);
        if (auto * ctx = request->scheduling_context)
        {
            auto & state = ctx->getResourceState(leaf);
            // Same corrected charge that advanced vruntime at push(), so the attained-service
            // threshold (`weight_lowering_io_bytes`) also tracks real cost, not the estimate.
            state.attained_cost += request->scheduling_charge;
            state.last_activity_ns = clock_gettime_ns();
        }
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
    /// Effective weight = base weight, lowered once by `weight_lowering_factor` as soon as the
    /// query crosses ANY configured threshold (age, attained CPU-seconds, or attained IO-bytes).
    /// Thresholds do not combine: the first to trip applies the full lowering.
    ///
    /// NOTE: this is sampled at `push()` time (when a request is keyed), so lowering applies from the
    /// threshold-crossing forward: requests a query already had queued when it crosses the threshold
    /// keep their full-weight `vstart` and are not re-ordered — only its subsequent requests advance
    /// virtual runtime at the lowered weight. The lag is bounded by the query's in-flight request
    /// count at the crossing, and lowering is a single-step heuristic bias, so this is acceptable
    /// (unlike `las`, where staleness is corrected on `pop()`).
    double effectiveWeight(const ResourceSchedulingContext & ctx, const ResourceSchedulingContext::ResourceState & state) const
    {
        bool lowered = false;

        // Real cumulative service for this query = attained_cost (charged at pop) plus the pending
        // real-vs-estimate correction not yet folded into it (see ResourceState::consumeCorrectedCost).
        // Peeking the pending correction here — instead of waiting for it to land in attained_cost at
        // the next pop — lets the attained-service thresholds react on the FIRST request after a
        // finish (no one-request lag), without mutating attained_cost (so a cancelled request that
        // never pops leaks nothing).
        const Int64 attained_service = state.attained_cost + state.cost_correction.load(std::memory_order_relaxed);

        if (ctx.weight_lowering_age_seconds > 0)
        {
            UInt64 now = clock_gettime_ns();
            double age_seconds = now > ctx.start_ns ? static_cast<double>(now - ctx.start_ns) / 1e9 : 0.0;
            if (age_seconds >= ctx.weight_lowering_age_seconds)
                lowered = true;
        }
        if (!lowered && unit == CostUnit::CPUNanosecond && ctx.weight_lowering_cpu_seconds > 0)
        {
            // attained_cost is the summed per-request cost, charged at grant (pop), so it measures
            // granted CPU service rather than CPU already spent: each preemptive lease renewal
            // charges the next quantum, so attained tracks granted CPU and leads actual consumption
            // by at most one quantum (cpu_slot_quantum_ns) per active slot (self-reconciled by the
            // lease's overrun term). This fair path runs for CPU only under slot preemption; without
            // preemption a CPU leaf falls back to fifo (see WorkloadNodeTraits::schedulerFor), so
            // this branch is not reached for CPU then.
            if (static_cast<double>(attained_service) / 1e9 >= ctx.weight_lowering_cpu_seconds)
                lowered = true;
        }
        if (!lowered && unit == CostUnit::IOByte && ctx.weight_lowering_io_bytes > 0)
        {
            if (static_cast<double>(attained_service) >= ctx.weight_lowering_io_bytes)
                lowered = true;
        }

        double weight = lowered ? ctx.weight * ctx.weight_lowering_factor : ctx.weight;
        return weight > 0 ? weight : 1e-9; // guard against division by zero
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling_key < rhs.scheduling_key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const CostUnit unit;
    const void * leaf; /// Identifies this leaf in the per-query context's per-resource map
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
/// needed. Requests with no query context are treated as level 0 (least attained) and stay FIFO.
class LasAlgorithm final : public ISchedulingAlgorithm
{
public:
    LasAlgorithm(CostUnit unit_, const void * leaf_)
        : leaf(leaf_)
        , base(baseQuantum(unit_))
    {
    }

    void push(ResourceRequest * request) override
    {
        Int64 attained = 0;
        if (auto * ctx = request->scheduling_context)
            attained = ctx->getResourceState(leaf).attained_cost;
        request->scheduling_key = {levelOf(attained), next_seq++};
        requests.insert(*request);
    }

    ResourceRequest * pop() override
    {
        // Lazy re-keying. A request is keyed by `levelOf(attained)` at ENQUEUE, but a query's
        // attained service grows as its OTHER requests are served, so a waiting request's stored
        // level goes stale. Attained service only ever grows, so the stored level can only be too
        // LOW (served too early), never too high. On pop, recompute the front request's level from
        // the query's CURRENT attained service; if it exceeds the stored level the request is not
        // actually the least-attained, so defer it — re-key to its true level and re-insert, then
        // take the next candidate. This restores true (bucketed) LAS when a query has several
        // requests queued at once (the IO case: concurrent socket ops have independent requests),
        // and is a no-op when it has one (CPU via CPULeaseAllocation). Bounded churn: geometric
        // levels mean a request is re-keyed at most ~log2(total_service) times over its lifetime,
        // and at most once per pop() (attained does not change while this loop runs). The FIFO
        // sequence tie-break (`scheduling_key.second`) is preserved, so intra-level arrival order
        // is kept.
        while (!requests.empty())
        {
            auto it = requests.begin();
            ResourceRequest * request = &*it;
            auto * ctx = request->scheduling_context;
            if (ctx)
            {
                double real_level = levelOf(ctx->getResourceState(leaf).attained_cost);
                if (real_level > request->scheduling_key.first)
                {
                    requests.erase(it);
                    request->scheduling_key.first = real_level;
                    requests.insert(*request);
                    continue;
                }
            }
            requests.erase(it);
            if (ctx)
            {
                auto & state = ctx->getResourceState(leaf);
                // Charge the declared cost corrected toward real consumption (see
                // ResourceState::consumeCorrectedCost): attained_cost (the LAS level key) tracks
                // real bytes/CPU long-term. Never negative, so the level never drops (no backward
                // motion); a refund is realized by a smaller charge on later requests.
                state.attained_cost += state.consumeCorrectedCost(request->scheduling_cost);
                state.last_activity_ns = clock_gettime_ns();
            }
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

    double levelOf(Int64 attained) const
    {
        if (attained <= 0)
            return 0.0;
        return std::floor(std::log2(1.0 + static_cast<double>(attained) / static_cast<double>(base)));
    }

    struct ByKey
    {
        bool operator()(const ResourceRequest & lhs, const ResourceRequest & rhs) const noexcept
        {
            return lhs.scheduling_key < rhs.scheduling_key;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    const void * leaf; /// Identifies this leaf in the per-query context's per-resource map
    const ResourceCost base;
    UInt64 next_seq = 0;
    Set requests;
};

/// `priority` — strict priority by the query's `priority` setting (the existing query setting,
/// reused). Lower value = higher precedence, served first; `priority = 0` ("no priority", the
/// default) and requests with no query context are treated as lowest precedence. Ties (equal
/// priority, incl. the all-zero default) fall back to FIFO by arrival. Like any strict-priority
/// scheme it can starve low-priority queries — `fair` is the non-starving alternative.
class PriorityAlgorithm final : public ISchedulingAlgorithm
{
public:
    void push(ResourceRequest * request) override
    {
        UInt64 priority = request->scheduling_context ? request->scheduling_context->priority : 0;
        // Map the `UInt64` `priority` query setting onto the Int64 `Priority` key (lower value =
        // higher precedence). `priority == 0` = "no priority" → the max key so it sorts strictly
        // last. Explicit priorities are clamped into `[1, max-1]` so (a) they always sort ahead of
        // "no priority" (0 and a huge explicit value no longer collide), and (b) a value above
        // `INT64_MAX` cannot wrap to a negative (spuriously high-precedence) key. This uses an
        // integer key rather than the `double` half of `scheduling_key`, which would lose ordering
        // above 2^53. Priorities beyond `INT64_MAX` are not distinguishable (the `Priority` type is
        // Int64), which is well beyond any realistic query priority. FIFO within equal priority.
        constexpr Int64 max_key = std::numeric_limits<Int64>::max();
        Int64 key = priority == 0
            ? max_key
            : static_cast<Int64>(std::min<UInt64>(priority, static_cast<UInt64>(max_key) - 1));
        request->scheduling_priority = Priority{key};
        request->scheduling_key = {0.0, next_seq++};
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
            if (lhs.scheduling_priority.value != rhs.scheduling_priority.value)
                return lhs.scheduling_priority.value < rhs.scheduling_priority.value;
            return lhs.scheduling_key.second < rhs.scheduling_key.second;
        }
    };
    using Set = boost::intrusive::set<ResourceRequest, ResourceRequest::SchedulingHook, boost::intrusive::compare<ByKey>>;

    UInt64 next_seq = 0;
    Set requests;
};

/*
 * Time-shared scheduler leaf that runs one of several pluggable scheduling algorithms, chosen by
 * the workload setting `scheduler` (default `fifo`). Replaces the standalone FifoQueue as the
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
        // `this` identifies this leaf in each query's per-resource scheduling state.
        algo = makeAlgorithm(algorithm_, unit_, this);
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
            if (value <= 0)
                throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Queue limit must be a positive value, got: {}", value);
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
        algo = makeAlgorithm(new_algorithm, unit, this);
        algorithm = new_algorithm;
        // `fair` projects each query's virtual runtime in push() and stores it in the (per-query)
        // scheduling context, which outlives this leaf's algorithm instances. The new instance
        // starts from a zero system virtual time, so reset each migrated query's projected vruntime
        // for this leaf before re-pushing — otherwise the pending backlog's projection would be
        // double-counted on top of what a previous `fair` stint left behind (re-keying the same
        // requests advances vruntime again). attained_cost is real accrued service, left untouched.
        // Resetting a context repeatedly is idempotent, so no dedup is needed before the push loop.
        //
        // Only queries that hold a pending request at the swap are reset here. A query that had
        // already drained its queue keeps the vruntime from the previous `fair` instance, so its
        // next request after a `fair -> other -> fair` toggle re-enters at max(system_vruntime,
        // stale_vruntime) and is transiently deprioritized until system_vruntime catches up. This
        // is bounded and self-correcting, and only reachable by live-reconfiguring a workload's
        // scheduler between `fair` stints; eliminating it entirely would require storing vruntime
        // per fair-instance (with drain-time cleanup to avoid an unbounded per-query map), which is
        // not worth the added state and lifetime complexity for so rare a case.
        // A request pulled from `fair` carries its real-vs-estimate correction already folded into
        // `scheduling_charge` (fair charges at push), while every other algorithm re-derives the
        // charge from `scheduling_cost` plus the shared `cost_correction` at pop and ignores
        // `scheduling_charge`. Return the consumed delta to the shared state before re-pushing, so a
        // live swap does not drop it. A no-op for a request coming from a pop-charging algorithm,
        // where `reset()` keeps `scheduling_charge == scheduling_cost`.
        for (ResourceRequest * request : pending)
            if (auto * ctx = request->scheduling_context)
            {
                auto & state = ctx->getResourceState(this);
                state.cost_correction.fetch_add(
                    static_cast<Int64>(request->scheduling_charge) - static_cast<Int64>(request->scheduling_cost),
                    std::memory_order_relaxed);
                request->scheduling_charge = request->scheduling_cost;
            }
        if (new_algorithm == SchedulerAlgorithm::Fair)
            for (ResourceRequest * request : pending)
                if (auto * ctx = request->scheduling_context)
                    ctx->getResourceState(this).vruntime = 0.0;
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
    static std::unique_ptr<ISchedulingAlgorithm> makeAlgorithm(SchedulerAlgorithm algorithm_, CostUnit unit_, const void * leaf_)
    {
        switch (algorithm_)
        {
            case SchedulerAlgorithm::Fifo:
                return std::make_unique<FifoAlgorithm>();
            case SchedulerAlgorithm::Fair:
                return std::make_unique<FairAlgorithm>(unit_, leaf_);
            case SchedulerAlgorithm::Las:
                return std::make_unique<LasAlgorithm>(unit_, leaf_);
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
