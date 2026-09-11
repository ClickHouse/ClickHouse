#pragma once

#include <base/types.h>
#include <Common/Scheduler/CostUnit.h>

#include <atomic>
#include <cstddef>
#include <memory>


namespace DB
{

/// Per-query, per-resource mutable scheduling state, used by the `fair` and `las` schedulers.
///
/// There is one instance per (query, scheduler leaf): a query uses several resources (CPU, IO
/// read, IO write, …) and each has its own leaf, hence its own state. Each instance is read and
/// written only by its owning leaf, under that leaf's mutex — the one exception is the
/// cross-thread `cost_correction` feed from `ResourceGuard::finish()`, hence the atomic.
///
/// Owned by the query's `ResourceSchedulingContext` in a store sized once when the classifier is
/// built and never resized, so the address of a slot is stable for the context's lifetime. The
/// classifier stamps a non-owning pointer to the right slot onto every `ResourceLink` it hands
/// out, and request tagging copies it onto `ResourceRequest::scheduling.state`, so a scheduler
/// reaches this state with a single dereference — no map, no lookup.
struct ResourceQueryState
{
    Int64 attained_cost = 0; /// Total dequeued cost for this query in this resource
    double vruntime = 0.0; /// SFQ virtual runtime for this query in this resource (`fair`)
    UInt64 last_activity_ns = 0; /// Monotonic time of the last dequeue (introspection)

    /// `fair`: cached effective weight plus a one-way "already lowered" latch. The effective
    /// weight is recomputed at push() only while `weight_lowered` is false; once a
    /// `weight_lowering_*` threshold trips, the lowered value is stored here and reused, so the
    /// threshold checks stop running. `effective_weight` is 0 until the first push() sets it.
    double effective_weight = 0.0;
    bool weight_lowered = false;

    /// Accumulated `real_cost - scheduling.cost` for this query's finished requests on this
    /// resource, not yet applied to a scheduling key. `ResourceGuard::Request::finish()` adds to
    /// it from the consumer thread (hence atomic; the other fields are touched only by the leaf
    /// thread). `fair`/`las` fold it into the NEXT request's charge in `consumeCorrectedCost()`,
    /// so per-query service tracks real cost long-term without ever rewriting an assigned key.
    std::atomic<Int64> cost_correction{0};

    /// Fold the accumulated correction into `base_cost` (the request's declared `scheduling.cost`)
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

/// Per-query scheduling context: the query-global configuration the `fair`/`priority` schedulers
/// read (weight, weight-lowering thresholds, priority), and the owner of this query's
/// per-resource `ResourceQueryState` slots.
///
/// Created once per query (or background activity) by the workload `Classifier` from the query's
/// scheduling settings and owned by the classifier for that lifetime. When the classifier has
/// finished attaching to resources it sizes the per-resource store (one slot per attached leaf)
/// and stamps a non-owning `ResourceQueryState *` plus this context onto every `ResourceLink` it
/// hands out; request tagging copies both onto the `ResourceRequest`. A request only reaches an
/// accounting leaf through a classifier link, so the `fair`/`las` schedulers can assume the
/// pointers are set.
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
        Int64 priority_)
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
    const Int64 priority; /// Query scheduling priority (setting `workload_priority`): lower = higher, 0 = neutral default, negatives outrank it. Used by the `priority` scheduler.

    /// Allocate the per-resource state slots (one per attached leaf). Called once by the classifier
    /// after it has attached to all resources, on the setup thread, before any request is enqueued.
    /// The store is never resized afterwards, so a `resourceState()` pointer stays valid for the
    /// context's lifetime. `ResourceQueryState` holds a `std::atomic` (non-movable) — which is
    /// exactly why it is sized once here rather than grown on demand.
    void initResourceStates(size_t count_)
    {
        per_resource = std::make_unique<ResourceQueryState[]>(count_);
        count = count_;
    }

    /// Non-owning pointer to the `index`-th per-resource state slot (stable for the context's lifetime).
    ResourceQueryState * resourceState(size_t index) { return &per_resource[index]; }

    size_t resourceCount() const { return count; }

private:
    std::unique_ptr<ResourceQueryState[]> per_resource;
    size_t count = 0;
};

using ResourceSchedulingContextPtr = std::shared_ptr<ResourceSchedulingContext>;

}
