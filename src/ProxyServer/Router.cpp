#include "Router.h"

#include <stdexcept>

#include <fmt/format.h>
#include <Poco/Exception.h>

#include <ProxyServer/ActiveConnections.h>
#include <ProxyServer/ConnectionsManager.h>
#include <ProxyServer/RoundRobin.h>
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
        return Action(RuleActionType::Reject, {}, rule->connections_manager, global_counter);
    }

    if (!rule->load_balancer)
    {
        // unlikely
        throw std::runtime_error(fmt::format("no balancer found for rule #{}", rule_id));
    }

    const auto route_to = rule->load_balancer->select(*rule->connections_manager);
    if (!route_to.has_value())
    {
        // unlikely
        throw std::runtime_error(fmt::format("balancer could not find a suitable server. rule #{}", rule_id));
    }

    const auto iter = servers.find(*route_to);
    if (iter == servers.end())
    {
        // unlikely
        throw std::runtime_error(fmt::format("balancer returned the server that is not in the servers container. rule #{}", rule_id));
    }

    return Action(RuleActionType::Route, {iter->second}, rule->connections_manager, global_counter);
}

}

Action::Action(
    RuleActionType type_,
    std::optional<ServerConfig> route_to_,
    std::shared_ptr<ActiveConnectionsManager> connections_manager_,
    GlobalConnectionsCounter * global_counter_)
    : type(type_)
    , route_to(std::move(route_to_))
    , global_counter(global_counter_)
{
    if (connections_manager_)
    {
        connections_manager = std::optional(std::weak_ptr<ActiveConnectionsManager>(connections_manager_));
    }
}

Action::~Action()
{
    disconnect();
}

RuleActionType Action::getType()
{
    return type;
}

const std::optional<ServerConfig> & Action::getRouteTo()
{
    return route_to;
}

void Action::disconnect()
{
    if (disconnected || !route_to.has_value() || !global_counter)
        return;
    disconnected = true;

    if (!connections_manager.has_value())
    {
        global_counter->updateConnectionCount(*route_to, -1);
        return;
    }

    auto connection_manager_ptr = connections_manager->lock();
    if (connection_manager_ptr)
    {
        connection_manager_ptr->removeConnection(*route_to);
    }
    else
    {
        global_counter->updateConnectionCount(*route_to, -1);
    }
}

Router::Router(const Poco::Util::AbstractConfiguration & cfg)
{
    updateConfig(cfg);
}

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

void Router::notifyConnectionFinished(std::weak_ptr<DefaultRule> rule, const ServerConfig & server)
{
    if (!rule.expired())
    {
        rule.lock()->connections_manager->removeConnection(server);
    }
    else
    {
        connections_counter.updateConnectionCount(server, -1);
    }
}

}
