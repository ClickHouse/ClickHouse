#pragma once

#include <base/types.h>
#include <Common/Scheduler/CostUnit.h>

#include <atomic>
#include <cstddef>
#include <memory>


namespace DB
{

/// Per-query, per-resource mutable scheduling state.
///
/// `attained_cost` is the query's real service in this resource, common to the query-aware
/// algorithms; `vruntime` (and its own correction) is `fair`'s alone. The two are tracked
/// independently — attained is charged generically when a request is served, vruntime only by
/// `fair`.
///
/// One instance per (query, leaf), owned by the query's `ResourceSchedulingContext` in a store
/// sized once when the classifier is built (never resized, so a slot's address is stable). The
/// classifier stamps a pointer to it onto every `ResourceLink`, and request tagging copies it onto
/// `ResourceRequest::scheduling.state`, so a scheduler reaches it with a single dereference — no
/// map, no lookup, no allocation on the hot path.
struct ResourceQueryState
{
    /// The query's accumulated service in this resource, tracking real cost. `RequestQueue::
    /// dequeueRequest` adds a request's declared cost when it is served (the estimate — real cost
    /// is not known yet), and `ResourceGuard::finish()` adds the real-minus-estimate delta once it
    /// is; so it converges to real service, counting an in-flight request at its estimate. Atomic
    /// (relaxed) because `finish()` runs on the consumer thread while the leaf reads/writes it on
    /// the scheduler thread. Read by `las` (level) and `fair` (weight-lowering thresholds).
    std::atomic<Int64> attained_cost{0};

    UInt64 last_activity_ns = 0; /// Monotonic time of the last dequeue (introspection); leaf thread only.

    /// `fair` only, leaf thread only. SFQ virtual runtime, plus a cached effective weight and a
    /// one-way "already lowered" latch: the weight is recomputed at push() until a
    /// `weight_lowering_*` threshold trips, then frozen (`effective_weight` is 0 until first set).
    double vruntime = 0.0;
    double effective_weight = 0.0;
    bool weight_lowered = false;

    /// `fair` only: pending `real_cost - scheduling.cost` correction for `vruntime`, independent of
    /// `attained_cost`. `finish()` adds the delta from the consumer thread (hence atomic); `fair`
    /// folds it into the next request's vruntime charge at push (`drainVruntimeCorrection`) and
    /// drains it. Kept separate from `attained_cost` so vruntime and attained service are computed
    /// independently.
    std::atomic<Int64> vruntime_correction{0};

    /// Fold the pending correction into `base_cost` to get the charge `fair` applies to `vruntime`.
    /// Never negative — vruntime is monotonic, so an over-estimate is realized by charging LESS on
    /// later requests, carrying any negative remainder forward until the charge converges to real
    /// cost. `fetch_sub` composes with a concurrent `finish()` `fetch_add`.
    ResourceCost drainVruntimeCorrection(ResourceCost base_cost)
    {
        Int64 corr = vruntime_correction.load(std::memory_order_relaxed);
        Int64 effective = static_cast<Int64>(base_cost) + corr;
        Int64 remainder = effective < 0 ? effective : 0; // negative part carried to the future
        vruntime_correction.fetch_sub(corr - remainder, std::memory_order_relaxed);
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
