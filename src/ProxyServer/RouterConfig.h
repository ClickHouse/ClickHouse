#pragma once

#include <unordered_map>
#include <vector>

#include <Poco/Util/AbstractConfiguration.h>

#include <ProxyServer/Rules.h>
#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

/// All upstream replicas, deduplicated by their `host:port` key. Used to resolve the key
/// chosen by a load balancer back to a concrete server.
using Servers = std::unordered_map<std::string, ServerConfig>;
/// An ordered list of the replicas that form a single named cluster.
using Cluster = std::vector<ServerConfig>;
/// Named clusters declared under `<upstreams><clusters>`, keyed by cluster name.
using Clusters = std::unordered_map<std::string, Cluster>;
using Rules = std::vector<std::shared_ptr<FilterRule>>;

struct RouterConfig
{
    Servers servers;
    Clusters clusters;
    Rules rules;
    std::shared_ptr<DefaultRule> default_rule;
};

RouterConfig parseConfig(const Poco::Util::AbstractConfiguration & config, GlobalConnectionsCounter * global_counter);

}
