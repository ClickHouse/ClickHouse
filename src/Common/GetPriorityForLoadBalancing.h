#pragma once

#include <Core/SettingsEnums.h>

namespace DB
{

class GetPriorityForLoadBalancing
{
public:
    explicit GetPriorityForLoadBalancing(LoadBalancing load_balancing_) : load_balancing(load_balancing_) {}
    GetPriorityForLoadBalancing() = default;

    GetPriorityForLoadBalancing(const GetPriorityForLoadBalancing & other)
        : hostname_prefix_distance(other.hostname_prefix_distance)
        , hostname_levenshtein_distance(other.hostname_levenshtein_distance)
        , load_balancing(other.load_balancing)
        , last_used(other.last_used.load())
    {
    }

    GetPriorityForLoadBalancing & operator=(const GetPriorityForLoadBalancing & other)
    {
        if (this != &other)
        {
            hostname_prefix_distance = other.hostname_prefix_distance;
            hostname_levenshtein_distance = other.hostname_levenshtein_distance;
            load_balancing = other.load_balancing;
            last_used = other.last_used.load();
        }
        return *this;
    }

    GetPriorityForLoadBalancing(GetPriorityForLoadBalancing && other) noexcept
        : hostname_prefix_distance(std::move(other.hostname_prefix_distance))
        , hostname_levenshtein_distance(std::move(other.hostname_levenshtein_distance))
        , load_balancing(other.load_balancing)
        , last_used(other.last_used.load())
    {
    }

    GetPriorityForLoadBalancing & operator=(GetPriorityForLoadBalancing && other) noexcept
    {
        if (this != &other)
        {
            hostname_prefix_distance = std::move(other.hostname_prefix_distance);
            hostname_levenshtein_distance = std::move(other.hostname_levenshtein_distance);
            load_balancing = other.load_balancing;
            last_used = other.last_used.load();
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

    std::function<Priority(size_t index)> getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const;

    std::vector<size_t> hostname_prefix_distance; /// Prefix distances from name of this host to the names of hosts of pools.
    std::vector<size_t> hostname_levenshtein_distance; /// Levenshtein Distances from name of this host to the names of hosts of pools.

    LoadBalancing load_balancing = LoadBalancing::RANDOM;

private:
    mutable std::atomic<size_t> last_used = 0; /// Last used for round_robin policy.
};

}
