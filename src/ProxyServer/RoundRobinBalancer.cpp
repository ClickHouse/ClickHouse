#include "RoundRobinBalancer.h"

namespace Proxy
{

RoundRobinLoadBalancer::RoundRobinLoadBalancer() = default;

RoundRobinLoadBalancer::~RoundRobinLoadBalancer() = default;


std::optional<std::string> RoundRobinLoadBalancer::select(const ConnectionsCounter & connections_counter)
{
    std::lock_guard<std::mutex> lock(mutex);
    return connections_counter.empty() ? std::nullopt : std::optional(connections_counter[counter++ % connections_counter.size()]);
}

}
