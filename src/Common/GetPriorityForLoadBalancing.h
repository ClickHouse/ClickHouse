#pragma once

#include <Common/Priority.h>
#include <Core/LoadBalancing.h>

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
    mutable size_t last_used = 0; /// Last used for round_robin policy.
};

}
