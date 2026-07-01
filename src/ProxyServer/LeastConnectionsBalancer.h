#pragma once

#include <mutex>

#include <ProxyServer/ILoadBalancer.h>

namespace Proxy
{

class LeastConnectionsLoadBalancer : public ILoadBalancer
{
public:
    LeastConnectionsLoadBalancer();
    ~LeastConnectionsLoadBalancer() override;

    std::optional<std::string> select(const ConnectionsCounter & connections_counter) override;

private:
    std::mutex mutex;
};

}
