#pragma once

#include <base/types.h>

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
        , weight(weight_ > 0 ? weight_ : 1.0)
        , weight_lowering_factor(weight_lowering_factor_)
        , weight_lowering_age_seconds(weight_lowering_age_seconds_)
        , weight_lowering_cpu_seconds(weight_lowering_cpu_seconds_)
        , weight_lowering_io_bytes(weight_lowering_io_bytes_)
        , priority(priority_)
    {
    }

    /// Immutable per-query configuration (from query settings at query start).
    const UInt64 start_ns; /// Monotonic `clock_gettime_ns()` when the query started; used for age
    const Float64 weight; /// Base fair-scheduling weight (query setting `weight`)
    const Float64 weight_lowering_factor; /// Multiply weight once a threshold trips (1 = disabled)
    const Float64 weight_lowering_age_seconds; /// Age threshold in seconds (0 = disabled)
    const Float64 weight_lowering_cpu_seconds; /// Attained CPU-seconds threshold (0 = disabled)
    const Float64 weight_lowering_io_bytes; /// Attained IO-bytes threshold (0 = disabled)
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
