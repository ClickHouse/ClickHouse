#include "Rules.h"

namespace Proxy
{

LoadBalancingPolicy parseLoadBalancingPolicy(const std::string & policy_str)
{
    if (policy_str == "")
        return defaultLoadBalancingPolicy;
    if (policy_str == "round_robin")
        return LoadBalancingPolicy::RoundRobin;
    if (policy_str == "active_connections")
        return LoadBalancingPolicy::ActiveConnections;
    throw std::invalid_argument("Unknown load balancing policy: " + policy_str);
}

}
