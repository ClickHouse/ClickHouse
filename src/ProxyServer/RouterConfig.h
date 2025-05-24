#pragma once

#include <unordered_map>
#include <vector>

#include <Poco/Util/AbstractConfiguration.h>

#include <ProxyServer/Rules.h>
#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

using Servers = std::unordered_map<std::string, ServerConfig>;
using Rules = std::vector<std::shared_ptr<FilterRule>>;

struct RouterConfig
{
    Servers servers;
    Rules rules;
    std::shared_ptr<DefaultRule> default_rule;
};

RouterConfig parseConfig(const Poco::Util::AbstractConfiguration & config, GlobalConnectionsCounter * global_counter);

}
