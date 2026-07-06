#pragma once

#include <optional>
#include <string>

#include <ProxyServer/ConnectionsCounter.h>

namespace Proxy
{

class ILoadBalancer
{
public:
    virtual ~ILoadBalancer() = default;

    virtual std::optional<std::string> select(const ConnectionsCounter & connections_counter) = 0;
};

}
