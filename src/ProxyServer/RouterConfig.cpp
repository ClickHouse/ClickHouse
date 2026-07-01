#include "RouterConfig.h"

#include <memory>

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/XML/XMLWriter.h>
#include <Common/Exception.h>

#include <ProxyServer/ConnectionsCounter.h>
#include <ProxyServer/LeastConnectionsBalancer.h>
#include <ProxyServer/RoundRobinBalancer.h>
#include <ProxyServer/Rules.h>
#include <ProxyServer/ServerConfig.h>

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
std::string makeServerKey(const std::string & host, int tcp_port)
{
    return host + ":" + std::to_string(tcp_port);
}

/// Parses the named clusters declared under `<upstreams><clusters>`. Every cluster is an
/// ordered list of `<replica>` entries (host and tcp_port). The flat `servers` map collects
/// all replicas across all clusters, deduplicated by their `host:port` key, so that the key
/// chosen by a load balancer can later be resolved back to a concrete server.
void parseClusters(const Poco::Util::AbstractConfiguration & config, Clusters & clusters, Servers & servers)
{
    std::vector<std::string> cluster_names;
    config.keys("upstreams.clusters", cluster_names);

    for (const std::string & cluster_name : cluster_names)
    {
        const std::string cluster_prefix = "upstreams.clusters." + cluster_name;

        Cluster cluster;

        std::vector<std::string> replica_keys;
        config.keys(cluster_prefix, replica_keys);

        for (const std::string & replica_key : replica_keys)
        {
            if (replica_key != "replica" && !replica_key.starts_with("replica["))
                continue;

            const std::string replica_prefix = cluster_prefix + "." + replica_key;

            ServerConfig server;
            server.host = config.getString(replica_prefix + ".host");
            server.tcp_port = config.getInt(replica_prefix + ".tcp_port", 0);
            server.key = makeServerKey(server.host, server.tcp_port);

            cluster.push_back(server);
            servers[server.key] = server;
        }

        if (cluster.empty())
        {
            throw DB::Exception(
                DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster '{}' has no replicas configured at {}", cluster_name, cluster_prefix);
        }

        clusters[cluster_name] = std::move(cluster);
    }
}

std::shared_ptr<DefaultRule> parseRule(
    const Poco::Util::AbstractConfiguration & config,
    const Clusters & clusters,
    GlobalConnectionsCounter * global_counter,
    const std::string & prefix,
    bool is_filter_rule)
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

    const std::string action_prefix = prefix + ".action";

    const bool has_reject = config.hasProperty(action_prefix + ".reject");
    const bool has_route_to = config.hasProperty(action_prefix + ".route_to");

    if (!has_reject && !has_route_to)
    {
        /// The default rule is optional: its absence simply means "reject everything that no
        /// rule matched". A filter rule, on the other hand, must declare an explicit action.
        if (!is_filter_rule)
            return nullptr;
        throw DB::Exception(
            DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Routing rule at {} must specify either 'reject' or 'route_to'", action_prefix);
    }

    if (has_reject && config.getBool(action_prefix + ".reject"))
    {
        rule->action.type = RuleActionType::Reject;
    }
    else if (has_route_to)
    {
        rule->action.type = RuleActionType::Route;

        const std::string cluster_name = config.getString(action_prefix + ".route_to");
        rule->action.target_cluster = cluster_name;

        const auto cluster_iter = clusters.find(cluster_name);
        if (cluster_iter == clusters.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Routing rule action 'route_to' references unknown cluster '{}' at {}",
                cluster_name,
                action_prefix);
        }

        const Cluster & cluster = cluster_iter->second;

        rule->action.target_servers.reserve(cluster.size());
        for (const ServerConfig & server : cluster)
            rule->action.target_servers.push_back(server.key);

        const std::string policy_str = config.getString(action_prefix + ".policy", "");
        rule->policy = parseLoadBalancingPolicy(policy_str);

        switch (rule->policy)
        {
            case LoadBalancingPolicy::RoundRobin:
                rule->load_balancer = std::make_unique<RoundRobinLoadBalancer>();
                break;
            case LoadBalancingPolicy::LeastConnections:
                rule->load_balancer = std::make_unique<LeastConnectionsLoadBalancer>();
                break;
        }

        rule->connections_counter = std::make_shared<ConnectionsCounter>(cluster, global_counter);
    }
    else
    {
        throw DB::Exception(
            DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Routing rule at {} must specify either 'reject: true' or 'route_to'", action_prefix);
    }

    return rule;
}

Rules parseRules(const Poco::Util::AbstractConfiguration & config, const Clusters & clusters, GlobalConnectionsCounter * global_counter)
{
    Rules rules;

    std::vector<std::string> config_keys;
    config.keys("routing.rules", config_keys);

    for (const std::string & config_key : config_keys)
    {
        if (config_key != "rule" && !config_key.starts_with("rule["))
            continue;

        std::string prefix = "routing.rules." + config_key;

        auto rule = parseRule(config, clusters, global_counter, prefix, /*is_filter_rule=*/true);
        rules.push_back(std::static_pointer_cast<FilterRule>(rule));
    }

    return rules;
}

std::shared_ptr<DefaultRule>
parseDefaultRule(const Poco::Util::AbstractConfiguration & config, const Clusters & clusters, GlobalConnectionsCounter * global_counter)
{
    std::string prefix = "routing.default";
    return parseRule(config, clusters, global_counter, prefix, /*is_filter_rule=*/false);
}
}

RouterConfig parseConfig(const Poco::Util::AbstractConfiguration & config, GlobalConnectionsCounter * global_counter)
{
    Clusters clusters;
    Servers servers;
    parseClusters(config, clusters, servers);

    auto rules = parseRules(config, clusters, global_counter);
    auto default_rule = parseDefaultRule(config, clusters, global_counter);

    return {
        .servers = std::move(servers),
        .clusters = std::move(clusters),
        .rules = std::move(rules),
        .default_rule = std::move(default_rule)};
}

}
