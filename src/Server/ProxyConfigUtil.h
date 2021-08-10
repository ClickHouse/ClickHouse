#pragma once

#include <map>
#include <memory>
#include <string>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class ProxyConfig;
using ProxyConfigs = std::map<std::string, std::unique_ptr<ProxyConfig>>;

namespace Util
{

ProxyConfigs parseProxies(const Poco::Util::AbstractConfiguration & config);
ProxyConfigs clone(const ProxyConfigs & proxies);

}

}
