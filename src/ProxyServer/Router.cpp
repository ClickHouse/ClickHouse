#include "Router.h"

#include <stdexcept>

#include <fmt/format.h>
#include <Poco/Exception.h>

#include <ProxyServer/ConnectionsCounter.h>
#include <ProxyServer/LeastConnectionsBalancer.h>
#include <ProxyServer/RoundRobinBalancer.h>
#include <ProxyServer/Rules.h>
#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

namespace
{

Action getActionByRule(
    std::shared_ptr<DefaultRule> rule, std::string && rule_id, const Servers & servers, GlobalConnectionsCounter * global_counter)
{
    if (rule->action.type == RuleActionType::Reject)
    {
        return Action(RuleActionType::Reject, {}, rule->connections_counter, global_counter);
    }

    if (!rule->load_balancer) [[unlikely]]
    {
        throw std::runtime_error(fmt::format("no balancer found for rule #{}", rule_id));
    }

    const auto target = rule->load_balancer->select(*rule->connections_counter);
    if (!target.has_value()) [[unlikely]]
    {
        throw std::runtime_error(fmt::format("balancer could not find a suitable server. rule #{}", rule_id));
    }

    const auto iter = servers.find(*target);
    if (iter == servers.end()) [[unlikely]]
    {
        throw std::runtime_error(fmt::format("balancer returned the server that is not in the servers container. rule #{}", rule_id));
    }

    /// Reserve the selected replica before handing out the RAII action. The matching decrement
    /// happens in `Action::disconnect`; without this reservation active load is never represented
    /// and `least_connections` cannot see the connections it is balancing.
    rule->connections_counter->addConnection(iter->second);

    return Action(RuleActionType::Route, {iter->second}, rule->connections_counter, global_counter);
}

}

Action::Action(
    RuleActionType type_,
    std::optional<ServerConfig> target_,
    std::shared_ptr<ConnectionsCounter> connections_counter_,
    GlobalConnectionsCounter * global_counter_)
    : type(type_)
    , target(std::move(target_))
    , global_counter(global_counter_)
{
    if (connections_counter_)
    {
        connections_counter = std::optional(std::weak_ptr<ConnectionsCounter>(connections_counter_));
    }
}

Action::Action(Action && other) noexcept
    : type(other.type)
    , target(std::move(other.target))
    , connections_counter(std::move(other.connections_counter))
    , global_counter(other.global_counter)
    , disconnected(other.disconnected)
{
    /// Neutralize the moved-from action so its destructor does not release the connection now owned here.
    other.target.reset();
    other.global_counter = nullptr;
    other.disconnected = true;
}

Action & Action::operator=(Action && other) noexcept
{
    if (this != &other)
    {
        /// Release the connection currently held by this action before taking over the other one.
        disconnect();

        type = other.type;
        target = std::move(other.target);
        connections_counter = std::move(other.connections_counter);
        global_counter = other.global_counter;
        disconnected = other.disconnected;

        other.target.reset();
        other.global_counter = nullptr;
        other.disconnected = true;
    }
    return *this;
}

Action::~Action()
{
    disconnect();
}

RuleActionType Action::getType()
{
    return type;
}

const std::optional<ServerConfig> & Action::getTarget()
{
    return target;
}

void Action::disconnect()
{
    if (disconnected || !target.has_value() || !global_counter)
        return;
    disconnected = true;

    if (!connections_counter.has_value())
    {
        global_counter->updateConnectionCount(*target, -1);
        return;
    }

    auto connection_counter_ptr = connections_counter->lock();
    if (connection_counter_ptr)
    {
        connection_counter_ptr->removeConnection(*target);
    }
    else
    {
        global_counter->updateConnectionCount(*target, -1);
    }
}

Router::Router(const Poco::Util::AbstractConfiguration & cfg)
{
    updateConfig(cfg);
}

Router::~Router() = default;

void Router::updateConfig(const Poco::Util::AbstractConfiguration & cfg)
{
    std::lock_guard lock(mutex);
    config = parseConfig(cfg, &connections_counter);
}

Action Router::route(const std::string & user, const std::string & hostname, const std::string & database)
{
    std::lock_guard lock(mutex);

    for (size_t rule_index = 0; rule_index < config.rules.size(); ++rule_index)
    {
        auto & rule = config.rules[rule_index];
        if (!rule->user.empty() && rule->user != user)
            continue;
        if (!rule->database.empty() && rule->database != database)
            continue;
        if (!rule->host.empty() && rule->host != hostname)
            continue;

        return getActionByRule(rule, std::to_string(rule_index), config.servers, &connections_counter);
    }

    if (config.default_rule)
    {
        return getActionByRule(config.default_rule, "default", config.servers, &connections_counter);
    }

    return Action(RuleActionType::Reject, {}, nullptr, nullptr);
}

}
