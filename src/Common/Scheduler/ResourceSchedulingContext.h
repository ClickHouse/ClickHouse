#pragma once

#include <base/types.h>
#include <Common/Scheduler/CostUnit.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{

/// Per-query scheduling context shared by all `ResourceRequest`s that one query emits.
///
/// Created once per query (on its `ThreadGroup`, from the query settings), owned by a
/// `shared_ptr` for the query lifetime; null for background thread groups (their requests are
/// scheduled anonymously). Every request the query submits carries a non-owning raw pointer to
/// it (`ResourceRequest::scheduling_context`).
///
/// It holds (1) immutable per-query configuration used by the `fair` scheduler to adjust the
/// query's weight as it runs, and (2) mutable per-resource scheduling state (attained cost,
/// virtual runtime) used by the `fair` and `las` schedulers. State is kept per resource because
/// a query uses several resources (CPU, IO read, IO write, …) and each has its own leaf.
class ResourceSchedulingContext
{
public:
    ResourceSchedulingContext(
        UInt64 start_ns_,
        Float64 weight_,
        Float64 weight_lowering_factor_,
        Float64 weight_lowering_age_seconds_,
        Float64 weight_lowering_cpu_seconds_,
        Float64 weight_lowering_io_bytes_,
        UInt64 priority_)
        : start_ns(start_ns_)
        // Non-positive weight is meaningless for SFQ (virtual runtime divides by the weight), so a
        // query setting `weight <= 0` falls back to the default 1.0.
        , weight(weight_ > 0 ? weight_ : 1.0)
        // Clamp to [0, 1]: the factor only ever LOWERS a query's weight. `1.0` disables lowering;
        // a value > 1 would raise the share (inverting the setting) and a negative value is
        // meaningless, so both ends are clamped to keep the user-facing contract monotonic.
        , weight_lowering_factor(weight_lowering_factor_ > 1.0 ? 1.0 : (weight_lowering_factor_ < 0.0 ? 0.0 : weight_lowering_factor_))
        // Thresholds are disabled at 0; a negative value is meaningless, so clamp it to 0 (disabled)
        // rather than storing a negative that only happens to read as disabled by the `> 0` checks.
        , weight_lowering_age_seconds(weight_lowering_age_seconds_ > 0 ? weight_lowering_age_seconds_ : 0.0)
        , weight_lowering_cpu_seconds(weight_lowering_cpu_seconds_ > 0 ? weight_lowering_cpu_seconds_ : 0.0)
        , weight_lowering_io_bytes(weight_lowering_io_bytes_ > 0 ? weight_lowering_io_bytes_ : 0.0)
        , priority(priority_)
    {
    }

    /// Immutable per-query configuration (from query settings at query start).
    const UInt64 start_ns; /// Monotonic `clock_gettime_ns()` when the query started; used for age
    const Float64 weight; /// Base fair-scheduling weight (query setting `weight`; non-positive → 1.0)
    const Float64 weight_lowering_factor; /// Multiply weight once a threshold trips (1 = disabled; clamped to [0,1])
    const Float64 weight_lowering_age_seconds; /// Age threshold in seconds (0 or negative = disabled)
    const Float64 weight_lowering_cpu_seconds; /// Attained CPU-seconds threshold (0 or negative = disabled)
    const Float64 weight_lowering_io_bytes; /// Attained IO-bytes threshold (0 or negative = disabled)
    const UInt64 priority; /// Query priority (setting `priority`): 1 highest, larger = lower, 0 = none. Used by the `priority` scheduler.

    /// Mutable per-resource scheduling state for this query. There is one entry per scheduler leaf
    /// the query uses (a leaf is per workload-resource), keyed by the leaf's address. Each entry is
    /// read and written only by its own leaf, under the leaf's mutex; the `mutex` below serializes
    /// only the first-touch insertion. `std::unordered_map` keeps references stable across
    /// insertions, so a leaf may cache the reference it gets from `getResourceState`.
    struct ResourceState
    {
        Int64 attained_cost = 0; /// Total dequeued cost for this query in this resource
        double vruntime = 0.0; /// SFQ virtual runtime for this query in this resource (`fair`)
        UInt64 last_activity_ns = 0; /// Monotonic time of the last dequeue (introspection)

        /// Accumulated `real_cost - scheduling_cost` for this query's finished requests on this
        /// resource, not yet applied to a scheduling key. `ResourceGuard::Request::finish()` adds to
        /// it from the consumer thread (hence atomic; the other fields are touched only by the leaf
        /// thread). `fair`/`las` fold it into the NEXT request's charge in `consumeCorrectedCost()`,
        /// so per-query service tracks real cost long-term without ever rewriting an assigned key.
        std::atomic<Int64> cost_correction{0};

        /// Fold the accumulated correction into `base_cost` (the request's declared `scheduling_cost`)
        /// to get the charge to apply to `vruntime`/`attained_cost` for the next request. The charge
        /// is never negative — a refund (over-estimate/failed op) is realized by charging LESS on
        /// subsequent requests, never by moving `vruntime`/`attained_cost` backward (which would break
        /// SFQ/LAS fairness). Any unspent negative remainder is carried forward, so long-term the
        /// cumulative charge converges to the cumulative real cost. `fetch_sub` composes correctly
        /// with a concurrent `finish()` `fetch_add`.
        ResourceCost consumeCorrectedCost(ResourceCost base_cost)
        {
            Int64 corr = cost_correction.load(std::memory_order_relaxed);
            Int64 effective = static_cast<Int64>(base_cost) + corr;
            Int64 remainder = effective < 0 ? effective : 0; // negative part carried to the future
            cost_correction.fetch_sub(corr - remainder, std::memory_order_relaxed);
            return static_cast<ResourceCost>(effective - remainder); // >= 0
        }
    };

    /// Returns this query's state for the leaf identified by `leaf`, creating it on first use.
    /// Thread-safe. The returned reference stays valid for the lifetime of the context.
    ResourceState & getResourceState(const void * leaf)
    {
        std::lock_guard lock(mutex);
        return per_resource[leaf];
    }

private:
    std::mutex mutex;
    std::unordered_map<const void *, ResourceState> per_resource;
};

using ResourceSchedulingContextPtr = std::shared_ptr<ResourceSchedulingContext>;

}
