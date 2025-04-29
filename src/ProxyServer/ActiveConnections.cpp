#include "ActiveConnections.h"

namespace Proxy
{

ActiveConnectionsLoadBalancer::ActiveConnectionsLoadBalancer() = default;

ActiveConnectionsLoadBalancer::~ActiveConnectionsLoadBalancer() = default;

std::optional<std::string> ActiveConnectionsLoadBalancer::select(const ActiveConnectionsManager & connections_manager)
{
    std::lock_guard<std::mutex> lock(mutex);
    return connections_manager.empty() ? std::nullopt : connections_manager.getLeastLoaded();
}
}
