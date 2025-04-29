#include "Router.h"

#include <stdexcept>

#include <fmt/format.h>
#include <Poco/Exception.h>

#include <ProxyServer/ActiveConnections.h>
#include <ProxyServer/RoundRobin.h>
#include <ProxyServer/Rules.h>
#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

namespace
{

Action getActionByRule(std::shared_ptr<DefaultRule> rule, std::string && rule_id, const Servers & servers)
{
    if (rule->action.type == RuleActionType::Reject)
    {
        return {.type = RuleActionType::Reject};
    }

    if (!rule->load_balancer)
    {
        // unlikely
        throw std::runtime_error(fmt::format("no balancer found for rule #{}", rule_id));
    }

    const auto route_to = rule->load_balancer->select(rule->connections_manager);
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

    return {.type = RuleActionType::Route, .route_to = {iter->second}};
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

        return getActionByRule(rule, std::to_string(rule_index), config.servers);
    }

    if (config.default_rule)
    {
        return getActionByRule(config.default_rule, "default", config.servers);
    }

    return {.type = RuleActionType::Reject};
}

void Router::notifyConnectionFinished(std::weak_ptr<DefaultRule> rule, const ServerConfig & server)
{
    if (!rule.expired())
    {
        rule.lock()->connections_manager.removeConnection(server);
    }
    else
    {
        connections_counter.updateConnectionCount(server, -1);
    }
}

}
