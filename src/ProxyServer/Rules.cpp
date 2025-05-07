#include "Rules.h"

namespace Proxy
{

LoadBalancingPolicy parseLoadBalancingPolicy(const std::string & policy_str)
{
    if (policy_str.empty())
        return defaultLoadBalancingPolicy;
    if (policy_str == "round_robin")
        return LoadBalancingPolicy::RoundRobin;
    if (policy_str == "least_connections")
        return LoadBalancingPolicy::LeastConnections;
    throw std::invalid_argument("Unknown load balancing policy: " + policy_str);
}

}
