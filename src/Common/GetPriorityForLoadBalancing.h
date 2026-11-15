#pragma once

#include <Common/Priority.h>
#include <Core/LoadBalancing.h>
#include <base/types.h>

#include <atomic>
#include <functional>
#include <vector>

namespace DB
{

class GetPriorityForLoadBalancing
{
public:
    using Func = std::function<Priority(size_t index)>;
    /// Returns the current number of in-flight requests to the pool with the given index.
    /// Used by the `least_request` load balancing (see `LoadBalancing::LEAST_REQUEST`).
    using GetActiveCountFunc = std::function<size_t(size_t index)>;

    explicit GetPriorityForLoadBalancing(LoadBalancing load_balancing_, size_t last_used_ = 0)
        : load_balancing(load_balancing_), last_used(last_used_)
    {
    }
    GetPriorityForLoadBalancing() = default;

    /// Explicit copy/move because `last_used` is a `std::atomic` and `std::atomic`
    /// is neither copyable nor movable. We propagate the current counter value
    /// with relaxed ordering — there is no happens-before relationship to preserve,
    /// the round-robin counter is only used as a rotation index.
    GetPriorityForLoadBalancing(const GetPriorityForLoadBalancing & other)
        : hostname_prefix_distance(other.hostname_prefix_distance)
        , hostname_levenshtein_distance(other.hostname_levenshtein_distance)
        , hostname_longest_common_prefix(other.hostname_longest_common_prefix)
        , hostname_longest_common_suffix(other.hostname_longest_common_suffix)
        , get_active_count(other.get_active_count)
        , load_balancing(other.load_balancing)
        , last_used(other.last_used.load(std::memory_order_relaxed))
    {
    }

    GetPriorityForLoadBalancing & operator=(const GetPriorityForLoadBalancing & other)
    {
        if (this != &other)
        {
            hostname_prefix_distance = other.hostname_prefix_distance;
            hostname_levenshtein_distance = other.hostname_levenshtein_distance;
            hostname_longest_common_prefix = other.hostname_longest_common_prefix;
            hostname_longest_common_suffix = other.hostname_longest_common_suffix;
            get_active_count = other.get_active_count;
            load_balancing = other.load_balancing;
            last_used.store(other.last_used.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    GetPriorityForLoadBalancing(GetPriorityForLoadBalancing && other) noexcept
        : hostname_prefix_distance(std::move(other.hostname_prefix_distance))
        , hostname_levenshtein_distance(std::move(other.hostname_levenshtein_distance))
        , hostname_longest_common_prefix(std::move(other.hostname_longest_common_prefix))
        , hostname_longest_common_suffix(std::move(other.hostname_longest_common_suffix))
        , get_active_count(std::move(other.get_active_count))
        , load_balancing(other.load_balancing)
        , last_used(other.last_used.load(std::memory_order_relaxed))
    {
    }

    GetPriorityForLoadBalancing & operator=(GetPriorityForLoadBalancing && other) noexcept
    {
        if (this != &other)
        {
            hostname_prefix_distance = std::move(other.hostname_prefix_distance);
            hostname_levenshtein_distance = std::move(other.hostname_levenshtein_distance);
            hostname_longest_common_prefix = std::move(other.hostname_longest_common_prefix);
            hostname_longest_common_suffix = std::move(other.hostname_longest_common_suffix);
            get_active_count = std::move(other.get_active_count);
            load_balancing = other.load_balancing;
            last_used.store(other.last_used.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    bool operator == (const GetPriorityForLoadBalancing & other) const
    {
        return load_balancing == other.load_balancing
            && hostname_prefix_distance == other.hostname_prefix_distance
            && hostname_levenshtein_distance == other.hostname_levenshtein_distance
            && hostname_longest_common_prefix == other.hostname_longest_common_prefix
            && hostname_longest_common_suffix == other.hostname_longest_common_suffix;
    }

    bool operator != (const GetPriorityForLoadBalancing & other) const
    {
        return !(*this == other);
    }

    Func getPriorityFunc(
        LoadBalancing load_balance,
        size_t offset,
        size_t pool_size,
        size_t least_request_choice_count = 2,
        Float64 least_request_active_request_bias = 1.0) const;

    bool hasOptimalNode() const;

    std::vector<size_t> hostname_prefix_distance; /// Prefix distances from name of this host to the names of hosts of pools.
    std::vector<size_t> hostname_levenshtein_distance; /// Levenshtein Distances from name of this host to the names of hosts of pools.
    std::vector<size_t> hostname_longest_common_prefix; /// Lengths of the longest common prefix of this host name and the names of hosts of pools.
    std::vector<size_t> hostname_longest_common_suffix; /// Lengths of the longest common suffix of this host name and the names of hosts of pools.

    /// Source of in-flight request counts for the `least_request` policy. May be empty
    /// (e.g. for ZooKeeper connections): then all pools are considered equally loaded
    /// and `least_request` degenerates to `random`. Not a part of operator ==: it is
    /// derived from the set of pools, which is already covered by the hostname vectors.
    GetActiveCountFunc get_active_count;

    LoadBalancing load_balancing = LoadBalancing::RANDOM;

private:
    /// Last used pool for round_robin policy. Atomic because `getPriorityFunc` is
    /// `const` and is called concurrently from many threads through
    /// `ConnectionPoolWithFailover::makeGetPriorityFunc` during distributed query
    /// dispatch (e.g. parallel replicas, distributed inserts). Without atomic
    /// access, two concurrent dispatches race on `++last_used` (TSan reports
    /// "data race", STID 4676-580d / 4676-58a7).
    mutable std::atomic<size_t> last_used = 0;
};

}
