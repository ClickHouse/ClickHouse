#include "LeastConnectionsBalancer.h"

namespace Proxy
{

LeastConnectionsLoadBalancer::LeastConnectionsLoadBalancer() = default;

LeastConnectionsLoadBalancer::~LeastConnectionsLoadBalancer() = default;

std::optional<std::string> LeastConnectionsLoadBalancer::select(const ConnectionsCounter & connections_counter)
{
    std::lock_guard<std::mutex> lock(mutex);
    return connections_counter.empty() ? std::nullopt : connections_counter.getLeastLoaded();
}
}
