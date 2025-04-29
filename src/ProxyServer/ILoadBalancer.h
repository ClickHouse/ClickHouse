#pragma once

#include <ProxyServer/ConnectionsManager.h>

namespace Proxy
{

class ILoadBalancer
{
public:
    virtual ~ILoadBalancer() = default;

    virtual std::optional<std::string> select(const ActiveConnectionsManager & connections_manager) = 0;
};

}
