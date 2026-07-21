#pragma once

#include <Common/Priority.h>
#include <Core/LoadBalancing.h>

#include <atomic>
#include <functional>
#include <vector>

namespace DB
{

class GetPriorityForLoadBalancing
{
public:
    using Func = std::function<Priority(size_t index)>;

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
            load_balancing = other.load_balancing;
            last_used.store(other.last_used.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    GetPriorityForLoadBalancing(GetPriorityForLoadBalancing && other) noexcept
        : hostname_prefix_distance(std::move(other.hostname_prefix_distance))
        , hostname_levenshtein_distance(std::move(other.hostname_levenshtein_distance))
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
            load_balancing = other.load_balancing;
            last_used.store(other.last_used.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    bool operator == (const GetPriorityForLoadBalancing & other) const
    {
        return load_balancing == other.load_balancing
            && hostname_prefix_distance == other.hostname_prefix_distance
            && hostname_levenshtein_distance == other.hostname_levenshtein_distance;
    }

    bool operator != (const GetPriorityForLoadBalancing & other) const
    {
        return !(*this == other);
    }

    Func getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const;

    bool hasOptimalNode() const;

    std::vector<size_t> hostname_prefix_distance; /// Prefix distances from name of this host to the names of hosts of pools.
    std::vector<size_t> hostname_levenshtein_distance; /// Levenshtein Distances from name of this host to the names of hosts of pools.

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
