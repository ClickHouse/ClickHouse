#pragma once

#include <base/types.h>
#include <Common/Scheduler/CostUnit.h>

#include <atomic>
#include <cstddef>
#include <memory>


namespace DB
{

/// Per-query, per-resource scheduling state. `attained_cost` is real service, common to all
/// query-aware algorithms; `vruntime` and its correction are `fair`'s alone. The two are tracked
/// independently. One instance per (query, leaf), owned by the query's `ResourceSchedulingContext`
/// in a store sized once at classifier build (never resized → stable address). A pointer to it is
/// stamped on the `ResourceLink` and copied onto `ResourceRequest::scheduling.state`, so a scheduler
/// reaches it with one dereference — no map or allocation on the hot path.
struct ResourceQueryState
{
    /// Real service, converging via estimate + correction: `dequeueRequest` adds the declared cost
    /// when a request is served, `ResourceGuard::finish()` adds the real-minus-estimate delta later.
    /// Atomic because `finish()` runs on the consumer thread, the leaf on the scheduler thread. Read
    /// by `las` (level) and `fair` (weight-lowering thresholds).
    std::atomic<Int64> attained_cost{0};

    UInt64 last_activity_ns = 0; /// Monotonic time of the last dequeue (introspection); leaf thread only.

    /// `fair` only, leaf thread only: SFQ virtual runtime, a cached effective weight, and a one-way
    /// latch — the weight is recomputed at push until a `weight_lowering_*` threshold trips, then frozen.
    double vruntime = 0.0;
    double effective_weight = 0.0;
    bool weight_lowered = false;

    /// `fair` only: pending `real - estimate` correction for `vruntime`, kept separate from
    /// `attained_cost` so the two are independent. `finish()` adds the delta (consumer thread, hence
    /// atomic); `fair::push` folds it into the next request's charge and drains it.
    std::atomic<Int64> vruntime_correction{0};

    /// Fold the pending correction into `base_cost` for `fair`'s vruntime charge. Never negative
    /// (vruntime is monotonic): an over-estimate is realized by charging less on later requests,
    /// carrying the negative remainder forward until it converges. `fetch_sub` composes with a
    /// concurrent `finish()` `fetch_add`.
    ResourceCost drainVruntimeCorrection(ResourceCost base_cost)
    {
        Int64 corr = vruntime_correction.load(std::memory_order_relaxed);
        Int64 effective = static_cast<Int64>(base_cost) + corr;
        Int64 remainder = effective < 0 ? effective : 0; // negative part carried to the future
        vruntime_correction.fetch_sub(corr - remainder, std::memory_order_relaxed);
        return static_cast<ResourceCost>(effective - remainder); // >= 0
    }
};

/// Per-query scheduling context: the query-global config the schedulers read (weight, thresholds,
/// priority) and the owner of this query's per-resource `ResourceQueryState` slots. Built once per
/// query by the workload `Classifier`; after attaching to all resources it sizes the store (one slot
/// per leaf) and stamps a `ResourceQueryState *` plus this context onto every `ResourceLink`. A
/// request reaches an accounting leaf only through such a link, so the schedulers assume both are set.
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
        // weight <= 0 is meaningless for SFQ (vruntime divides by it) → default 1.0.
        , weight(weight_ > 0 ? weight_ : 1.0)
        // The factor only ever LOWERS the weight → clamp to [0, 1] (1 disables, >1 would invert, <0 is meaningless).
        , weight_lowering_factor(weight_lowering_factor_ > 1.0 ? 1.0 : (weight_lowering_factor_ < 0.0 ? 0.0 : weight_lowering_factor_))
        // Thresholds: 0 (or negative, clamped) disables.
        , weight_lowering_age_seconds(weight_lowering_age_seconds_ > 0 ? weight_lowering_age_seconds_ : 0.0)
        , weight_lowering_cpu_seconds(weight_lowering_cpu_seconds_ > 0 ? weight_lowering_cpu_seconds_ : 0.0)
        , weight_lowering_io_bytes(weight_lowering_io_bytes_ > 0 ? weight_lowering_io_bytes_ : 0.0)
        , priority(priority_)
    {
    }

    /// Immutable per-query config (from query settings at query start).
    const UInt64 start_ns; /// Monotonic query start (`clock_gettime_ns()`), for age
    const Float64 weight; /// Base fair weight (non-positive → 1.0)
    const Float64 weight_lowering_factor; /// Multiplies weight once a threshold trips (1 = disabled)
    const Float64 weight_lowering_age_seconds; /// Age threshold, seconds (0 = disabled)
    const Float64 weight_lowering_cpu_seconds; /// Attained CPU-seconds threshold (0 = disabled)
    const Float64 weight_lowering_io_bytes; /// Attained IO-bytes threshold (0 = disabled)
    const Int64 priority; /// `priority` scheduler order (`workload_priority`): lower = higher, 0 neutral, negatives outrank it

    /// Allocate the per-resource slots (one per attached leaf). Called once by the classifier after
    /// attaching to all resources, before any request is enqueued; never resized, so a `resourceState()`
    /// pointer stays valid for the context's lifetime (a `unique_ptr<T[]>` rather than a growable
    /// vector because `ResourceQueryState` holds a non-movable `std::atomic`).
    void initResourceStates(size_t count_)
    {
        per_resource = std::make_unique<ResourceQueryState[]>(count_);
        count = count_;
    }

    /// Non-owning pointer to the `index`-th per-resource slot (stable for the context's lifetime).
    ResourceQueryState * resourceState(size_t index) { return &per_resource[index]; }

    size_t resourceCount() const { return count; }

private:
    std::unique_ptr<ResourceQueryState[]> per_resource;
    size_t count = 0;
};

using ResourceSchedulingContextPtr = std::shared_ptr<ResourceSchedulingContext>;

}
