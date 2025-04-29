#pragma once

#include <mutex>

#include <Poco/Util/AbstractConfiguration.h>

#include <ProxyServer/ConnectionsManager.h>
#include <ProxyServer/RouterConfig.h>
#include <ProxyServer/Rules.h>

namespace Proxy
{

struct Action
{
    RuleActionType type;
    std::optional<ServerConfig> route_to = {};
};

class Router
{
private:
    using Config = RouterConfig;

public:
    Router(const Router &) = delete;
    Router & operator=(const Router &) = delete;
    Router(const Router &&) = delete;
    Router & operator=(Router &&) = delete;
    ~Router() = default;

    explicit Router(const Poco::Util::AbstractConfiguration & cfg);

    void updateConfig(const Poco::Util::AbstractConfiguration & cfg);

    Action route(const std::string & user, const std::string & hostname, const std::string & database);

    void notifyConnectionFinished(std::weak_ptr<DefaultRule> rule, const ServerConfig & server);

private:
    RouterConfig config;

    GlobalConnectionsCounter connections_counter;
    mutable std::mutex mutex;
};

using RouterPtr = std::shared_ptr<Router>;

}
