#include <Server/ProxyConfigUtil.h>
#include <Server/ProxyConfig.h>
#include <Server/PROXYProxyConfig.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

namespace Util
{

namespace
{

std::unique_ptr<ProxyConfig> makeProxy(
    const std::string & name,
    const std::string & protocol
)
{
    if (boost::iequals(protocol, "PROXY")) return std::make_unique<PROXYProxyConfig>(name);

    throw Exception("Unknown proxy protocol '" + protocol + "'", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
}

std::unique_ptr<ProxyConfig> parseProxy(
    const std::string & name,
    const std::string & protocol,
    const Poco::Util::AbstractConfiguration & config
)
{
    auto proxy = makeProxy(name, protocol);
    proxy->updateConfig(config);
    return proxy;
}

}

ProxyConfigs parseProxies(const Poco::Util::AbstractConfiguration & config)
{
    ProxyConfigs proxies;

    const auto add_proxy = [&] (std::unique_ptr<ProxyConfig> && proxy)
    {
        if (!proxy)
            return;

        if (proxies.count(boost::to_lower_copy(proxy->name)))
            throw Exception("Proxy name '" + proxy->name + "' already in use", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        proxies.emplace(proxy->name, std::move(proxy));
    };

    if (!config.has("proxies"))
        return proxies;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("proxies", keys);

    for (const auto & key : keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            throw Exception("Proxy name '" + key.substr(0, bracket_pos) + "' already in use", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        const auto prefix = "proxies." + key;

        if (!config.has(prefix + ".protocol"))
            throw Exception("Missing protocol for " + key + " proxy", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        const auto protocol = config.getString(prefix + ".protocol");

        Poco::AutoPtr<Poco::Util::AbstractConfiguration> proxy_config(
            const_cast<Poco::Util::AbstractConfiguration &>(config).createView(prefix));
        add_proxy(parseProxy(key, protocol, *proxy_config));
    }

    return proxies;
}

ProxyConfigs clone(const ProxyConfigs & proxies)
{
    ProxyConfigs cloned;

    for (const auto & pair : proxies)
    {
        cloned.emplace(pair.first, pair.second->clone());
    }

    return cloned;
}

}

}
