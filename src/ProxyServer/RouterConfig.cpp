#include "RouterConfig.h"

#include <memory>

#include <ProxyServer/ActiveConnections.h>
#include <ProxyServer/RoundRobin.h>
#include <ProxyServer/Rules.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLWriter.h>
#include "Common/Exception.h"
#include "ProxyServer/ConnectionsManager.h"
#include "ProxyServer/ServerConfig.h"

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}
}

namespace Proxy
{

namespace
{
Servers parseServers(const Poco::Util::AbstractConfiguration & config)
{
    Servers servers;

    std::vector<std::string> config_keys;
    config.keys("storage.servers", config_keys);

    for (const std::string & config_key : config_keys)
    {
        if (config_key != "server" && !config_key.starts_with("server["))
            continue;

        try
        {
            const auto prefix = "storage.servers." + config_key;
            ServerConfig server;
            server.key = config.getString(prefix + ".key");
            server.host = config.getString(prefix + ".host");
            server.http_port = config.getInt(prefix + ".http-port");
            server.tcp_port = config.getInt(prefix + ".tcp-port");
            servers[server.key] = std::move(server);
        }
        catch (const Poco::NotFoundException &)
        {
            break;
        }
    }

    return servers;
}

std::shared_ptr<DefaultRule>
parseRule(const Poco::Util::AbstractConfiguration & config, const Servers & servers, GlobalConnectionsCounter * global_counter, std::string prefix, bool is_filter_rule)
try
{
    std::shared_ptr<DefaultRule> rule;

    if (is_filter_rule)
    {
        auto filter_rule = std::make_shared<FilterRule>();
        filter_rule->database = config.getString(prefix + ".database", "");
        filter_rule->host = config.getString(prefix + ".host", "");
        filter_rule->user = config.getString(prefix + ".user", "");
        rule = std::move(filter_rule);
    }
    else
    {
        rule = std::make_shared<DefaultRule>();
    }

    prefix += ".action";

    if (config.hasProperty(prefix + ".reject") && config.getBool(prefix + ".reject"))
    {
        rule->action.type = RuleActionType::Reject;
    }
    else
    {
        prefix += ".route_to";
        rule->action.type = RuleActionType::Route;
        std::vector<std::string> config_keys;
        config.keys(prefix, config_keys);

        for (const std::string & config_key : config_keys)
        {
            if (config_key != "server" && !config_key.starts_with("server["))
                continue;

            try
            {
                std::string server_key = config.getString(prefix + "." + config_key);

                rule->action.route_to_servers.push_back(server_key);
            }
            catch (const Poco::NotFoundException &)
            {
                break;
            }
        }

        if (rule->action.route_to_servers.empty())
        {
            throw DB::Exception(
                DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Routing rule action 'route_to' has no servers configured at {}", prefix);
        }

        std::string policy_str = config.getString(prefix + ".action.policy", "");
        rule->policy = parseLoadBalancingPolicy(policy_str);

        switch (rule->policy)
        {
            case LoadBalancingPolicy::RoundRobin:
                rule->load_balancer = std::make_unique<RoundRobinLoadBalancer>();
                break;
            case LoadBalancingPolicy::ActiveConnections:
                rule->load_balancer = std::make_unique<ActiveConnectionsLoadBalancer>();
                break;
        }

        std::vector<ServerConfig> rule_servers;
        rule_servers.reserve(rule->action.route_to_servers.size());
        for (const auto & server_key : rule->action.route_to_servers)
        {
            const auto iter = servers.find(server_key);
            if (iter == servers.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Server with key '{}' not found in configuration", server_key);
            }
            rule_servers.push_back(iter->second);
        }
        rule->connections_manager = std::make_shared<ActiveConnectionsManager>(rule_servers, global_counter);
    }

    return rule;
}
catch (const Poco::NotFoundException &)
{
    return nullptr;
}

Rules parseRules(const Poco::Util::AbstractConfiguration & config, const Servers & servers, GlobalConnectionsCounter * global_counter)
{
    Rules rules;

    std::vector<std::string> config_keys;
    config.keys("routing.rules", config_keys);

    for (const std::string & config_key : config_keys)
    {
        if (config_key != "rule" && !config_key.starts_with("rule["))
            continue;

        std::string prefix = "routing.rules." + config_key;

        auto rule = parseRule(config, servers, global_counter, prefix, /*is_filter_rule=*/true);
        if (rule == nullptr)
        {
            break;
        }

        rules.push_back(std::static_pointer_cast<FilterRule>(rule));
    }

    return rules;
}

std::shared_ptr<DefaultRule> parseDefaultRule(const Poco::Util::AbstractConfiguration & config, const Servers & servers, GlobalConnectionsCounter * global_counter)
{
    std::string prefix = "routing.default";
    return parseRule(config, servers, global_counter, prefix, /*is_filter_rule=*/false);
}
}

RouterConfig parseConfig(const Poco::Util::AbstractConfiguration & config, GlobalConnectionsCounter * global_counter)
{
    auto servers = parseServers(config);
    auto rules = parseRules(config, servers, global_counter);
    auto default_rule = parseDefaultRule(config, servers, global_counter);

    return {.servers = std::move(servers), .rules = std::move(rules), .default_rule = std::move(default_rule)};
}

}
