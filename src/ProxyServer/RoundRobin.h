#pragma once

#include <mutex>

#include <ProxyServer/ILoadBalancer.h>

namespace Proxy
{

class RoundRobinLoadBalancer : public ILoadBalancer
{
public:
    RoundRobinLoadBalancer();
    ~RoundRobinLoadBalancer() override;

    std::optional<std::string> select(const ActiveConnectionsManager & connections_manager) override;

private:
    size_t counter = 0;
    std::mutex mutex;
};

}
