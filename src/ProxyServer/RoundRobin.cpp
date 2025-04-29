#include "RoundRobin.h"

namespace Proxy
{


RoundRobinLoadBalancer::RoundRobinLoadBalancer() = default;

RoundRobinLoadBalancer::~RoundRobinLoadBalancer() = default;


std::optional<std::string> RoundRobinLoadBalancer::select(const ActiveConnectionsManager & connections_manager)
{
    std::lock_guard<std::mutex> lock(mutex);
    return connections_manager.empty() ? std::nullopt : std::optional(connections_manager[counter++ % connections_manager.size()]);
}

}
