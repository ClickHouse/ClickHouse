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
public:
    explicit Action(
        RuleActionType type_,
        std::optional<ServerConfig> route_to_,
        std::shared_ptr<ActiveConnectionsManager> connections_manager_,
        GlobalConnectionsCounter * global_counter_);
    ~Action();

    Action(const Action &) = delete; // non-copyable
    Action & operator=(const Action &) = delete;
    Action(Action &&) = default; // movable
    Action & operator=(Action &&) = default;

    RuleActionType getType();
    const std::optional<ServerConfig>& getRouteTo();
    
    void disconnect();

private:
    RuleActionType type;
    std::optional<ServerConfig> route_to = {};

    std::optional<std::weak_ptr<ActiveConnectionsManager>> connections_manager = {};
    GlobalConnectionsCounter * global_counter = nullptr;
    bool disconnected = false;
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
