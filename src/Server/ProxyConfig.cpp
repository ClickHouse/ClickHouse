#include <Server/ProxyConfig.h>
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
}

PROXYConfig::PROXYConfig(const std::string & name_)
    : ProxyConfig(name_, "PROXY")
{
}

std::unique_ptr<ProxyConfig> PROXYConfig::clone() const
{
    return std::make_unique<PROXYConfig>(*this);
}

void PROXYConfig::updateConfig(const Poco::Util::AbstractConfiguration & config)
{
    ProxyConfig::updateConfig(config);

    if (config.has("version"))
    {
        const auto version_num = config.getUInt("version");
        switch (version_num)
        {
            case 1: version = Version::v1; break;
            case 2: version = Version::v2; break;

            default:
                throw Exception("Bad PROXY protocol version " + std::to_string(version_num), ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    if (config.has("trust"))
    {
        nets.clear();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("trust", keys);

        for (const auto & key_orig : keys)
        {
            auto key = boost::to_lower_copy(key_orig);

            const auto bracket_pos = key.find('[');
            if (bracket_pos != std::string::npos)
                key.resize(bracket_pos);

            if (key != "net")
                throw Exception{"Unexpected key '" + key_orig + "' in the list of IP networks (expecting 'net' entries each with a CIDR IP network address)", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

            nets.push_back(config.getString("trust." + key_orig));
        }
    }

    if (config.has("allow_http_x_forwarded_for"))
        allow_http_x_forwarded_for = config.getBool("allow_http_x_forwarded_for");
}

namespace Util
{

std::unique_ptr<ProxyConfig> makeProxy(
    const std::string & name,
    const std::string & protocol
)
{
    if (boost::iequals(protocol, "PROXY")) return std::make_unique<PROXYConfig>(name);

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

std::map<std::string, std::unique_ptr<ProxyConfig>> parseProxies(const Poco::Util::AbstractConfiguration & config)
{
    std::map<std::string, std::unique_ptr<ProxyConfig>> proxies;

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

}

}
