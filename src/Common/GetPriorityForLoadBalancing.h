#pragma once

#include <Core/SettingsEnums.h>

namespace DB
{

class GetPriorityForLoadBalancing
{
public:
    GetPriorityForLoadBalancing(LoadBalancing load_balancing_) : load_balancing(load_balancing_) {}
    GetPriorityForLoadBalancing(){}

    bool operator == (const GetPriorityForLoadBalancing & other) const
    {
        return load_balancing == other.load_balancing && hostname_differences == other.hostname_differences;
    }

    bool operator != (const GetPriorityForLoadBalancing & other) const
    {
        return !(*this == other);
    }

    std::function<size_t(size_t index)> getPriorityFunc(LoadBalancing load_balance, size_t offset, size_t pool_size) const;

    std::vector<size_t> hostname_differences; /// Distances from name of this host to the names of hosts of pools.

    LoadBalancing load_balancing = LoadBalancing::RANDOM;

private:
    mutable size_t last_used = 0; /// Last used for round_robin policy.
};

}
