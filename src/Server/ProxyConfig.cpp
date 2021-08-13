#include <Server/ProxyConfig.h>
#include <Server/PROXYProxyConfig.h>
#include <Server/HTTPProxyConfig.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

ProxyConfig::ProxyConfig(const std::string & name_, const std::string & protocol_)
    : name(name_)
    , protocol(protocol_)
{
}

void ProxyConfig::updateConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("protocol") && !boost::iequals(config.getString("protocol"), protocol))
        throw Exception("Cannot modify previously configured protocol", ErrorCodes::INVALID_CONFIG_PARAMETER);

    if (config.has("trust"))
    {
        trusted_networks.clear();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("trust", keys);

        for (const auto & key_orig : keys)
        {
            auto key = boost::to_lower_copy(key_orig);

            const auto bracket_pos = key.find('[');
            if (bracket_pos != std::string::npos)
                key.resize(bracket_pos);

            if (key != "net")
                throw Exception{"Unexpected key '" + key_orig + "' in the list of IP networks (expecting 'net' entries each with a IPv4 or IPv6 network address)", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

            const auto net_str = config.getString("trust." + key_orig);

            boost::system::error_code ec4;
            const auto net4 = boost::asio::ip::make_network_v4(net_str, ec4);

            if (ec4)
            {
                boost::system::error_code ec6;
                const auto net6 = boost::asio::ip::make_network_v6(net_str, ec6);

                if (ec6)
                    throw Exception{"Unable to interpret '" + net_str + "' as a IPv4 or IPv6 network address", ErrorCodes::INVALID_CONFIG_PARAMETER};
                else
                    trusted_networks.push_back(net6);
            }
            else
                trusted_networks.push_back(net4);
        }
    }
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
    if (boost::iequals(protocol, "HTTP")) return std::make_unique<HTTPProxyConfig>(name);

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

bool addrInNet(const boost::asio::ip::address_v4 & address, const boost::asio::ip::network_v4 & network)
{
    const auto range = network.hosts();
    return range.find(address) != range.end();
}

bool addrInNet(const boost::asio::ip::address_v6 & address, const boost::asio::ip::network_v6 & network)
{
    const auto range = network.hosts();
    return range.find(address) != range.end();
}

bool addrInNet(const boost::asio::ip::address & address, const ProxyConfig::Network & network)
{
    return std::visit(
        [&] (const auto & net)
        {
            return (address.is_v4() ? addrInNet(address.to_v4(), net) : addrInNet(address.to_v6(), net));
        },
        network
    );
}

bool addrInNet(const boost::asio::ip::address & address, const ProxyConfig::Networks & networks)
{
    for (const auto & network : networks)
    {
        if (addrInNet(address, network))
            return true;
    }
    return false;
}

}

}
