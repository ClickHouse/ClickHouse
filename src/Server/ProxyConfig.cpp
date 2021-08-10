#include <Server/ProxyConfig.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
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

}
