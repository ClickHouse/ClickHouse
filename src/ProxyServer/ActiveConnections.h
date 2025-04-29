#pragma once

#include <mutex>

#include <ProxyServer/ILoadBalancer.h>

namespace Proxy
{

class ActiveConnectionsLoadBalancer : public ILoadBalancer
{
public:
    ActiveConnectionsLoadBalancer();
    ~ActiveConnectionsLoadBalancer() override;

    std::optional<std::string> select(const ActiveConnectionsManager & connections_manager) override;

private:
    std::mutex mutex;
};

}
