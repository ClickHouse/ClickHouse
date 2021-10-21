#pragma once

#include <Core/SettingsEnums.h>

namespace DB
{

class GetPriorityForLoadBalancing
{
public:
    GetPriorityForLoadBalancing(LoadBalancing load_balancing_, size_t pool_size_) : load_balancing(load_balancing_), pool_size(pool_size_) {}
    GetPriorityForLoadBalancing(){}

    bool operator!=(const GetPriorityForLoadBalancing & other)
    {
        return offset != other.offset || pool_size != other.pool_size || load_balancing != other.load_balancing
            || hostname_differences != other.hostname_differences;
    }

    std::function<size_t(size_t index)> getPriorityFunc() const;

    LoadBalancing load_balancing = LoadBalancing::RANDOM;

    std::vector<size_t> hostname_differences; /// Distances from name of this host to the names of hosts of pools.
    size_t offset = 0; /// for first_or_random policy.
    size_t pool_size = 0; /// for round_robin policy.

private:
    mutable size_t last_used = 0; /// Last used for round_robin policy.
};

}
