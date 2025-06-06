#pragma once

#include <mutex>

#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <ProxyServer/ConnectionsCounter.h>
#include <ProxyServer/RouterConfig.h>
#include <ProxyServer/Rules.h>

namespace Proxy
{

struct Action
{
public:
    explicit Action(
        RuleActionType type_,
        std::optional<ServerConfig> target_,
        std::shared_ptr<ConnectionsCounter> connections_counter_,
        GlobalConnectionsCounter * global_counter_);
    ~Action();

    Action(const Action &) = delete; // non-copyable
    Action & operator=(const Action &) = delete; // non-copyable
    Action(Action &&) = default; // movable
    Action & operator=(Action &&) = default; // movable

    RuleActionType getType();
    const std::optional<ServerConfig> & getTarget();

    void disconnect();

private:
    RuleActionType type;
    std::optional<ServerConfig> target = {};

    std::optional<std::weak_ptr<ConnectionsCounter>> connections_counter = {};
    GlobalConnectionsCounter * global_counter = nullptr;
    bool disconnected = false;
};

class Router : private boost::noncopyable
{
private:
    using Config = RouterConfig;

public:
    explicit Router(const Poco::Util::AbstractConfiguration & cfg);
    ~Router();

    void updateConfig(const Poco::Util::AbstractConfiguration & cfg);

    Action route(const std::string & user, const std::string & hostname, const std::string & database);

private:
    RouterConfig config;

    GlobalConnectionsCounter connections_counter;
    mutable std::mutex mutex;
};

using RouterPtr = std::shared_ptr<Router>;

}
