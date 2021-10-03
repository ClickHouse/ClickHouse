#include <Server/InterfaceConfig.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Interpreters/Context.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <base/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NETWORK_ERROR;
}

LegacyGlobalConfigOverrides::LegacyGlobalConfigOverrides(const Poco::Util::AbstractConfiguration & config)
{
    listen_host = DB::getMultipleValuesFromConfig(config, "", "listen_host");

    if (config.has("listen_try"))
        listen_try = config.getBool("listen_try");

    if (listen_host.empty())
    {
        listen_host.emplace_back("::1");
        listen_host.emplace_back("127.0.0.1");
        listen_try = true;
    }

    if (config.has("listen_reuse_port"))
        listen_reuse_port = config.getBool("listen_reuse_port");

    if (config.has("listen_backlog"))
        listen_reuse_port = config.getUInt("listen_backlog");


    if (config.has("connection_timeout"))
        connection_timeout = std::chrono::seconds(config.getUInt("connection_timeout"));

    if (config.has("send_timeout"))
        send_timeout = std::chrono::seconds(config.getUInt("send_timeout"));

    if (config.has("receive_timeout"))
        receive_timeout = std::chrono::seconds(config.getUInt("receive_timeout"));

    if (config.has("keep_alive_timeout"))
        keep_alive_timeout = std::chrono::seconds(config.getUInt("keep_alive_timeout"));


    if (config.has("tcp_connection_timeout"))
        tcp_connection_timeout = std::chrono::seconds(config.getUInt("tcp_connection_timeout"));

    if (config.has("tcp_send_timeout"))
        tcp_send_timeout = std::chrono::seconds(config.getUInt("tcp_send_timeout"));

    if (config.has("tcp_receive_timeout"))
        tcp_receive_timeout = std::chrono::seconds(config.getUInt("tcp_receive_timeout"));

    if (config.has("tcp_keep_alive_timeout"))
        tcp_keep_alive_timeout = std::chrono::seconds(config.getUInt("tcp_keep_alive_timeout"));


    if (config.has("http_connection_timeout"))
        http_connection_timeout = std::chrono::seconds(config.getUInt("http_connection_timeout"));

    if (config.has("http_send_timeout"))
        http_send_timeout = std::chrono::seconds(config.getUInt("http_send_timeout"));

    if (config.has("http_receive_timeout"))
        http_receive_timeout = std::chrono::seconds(config.getUInt("http_receive_timeout"));

    if (config.has("http_keep_alive_timeout"))
        http_keep_alive_timeout = std::chrono::seconds(config.getUInt("http_keep_alive_timeout"));
}


InterfaceConfig::InterfaceConfig(const std::string & name_, const std::string & protocol_)
    : name(name_)
    , protocol(protocol_)
{
}

void InterfaceConfig::updateConfig(
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs & proxies_,
    const Settings & settings
)
{
    if (config.has("protocol") && !boost::iequals(config.getString("protocol"), protocol))
        throw Exception("Cannot modify previously configured protocol", ErrorCodes::INVALID_CONFIG_PARAMETER);

    updateConfig(global_overrides);
    updateConfig(config, proxies_);
    updateConfig(settings);
}

MultiEndpointInterfaceConfigBase::MultiEndpointInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : InterfaceConfig(name_, protocol_)
{
}

void MultiEndpointInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs &
)
{
    if (config.has("listen"))
    {
        hosts.clear();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("listen", keys);

        for (const auto & key_orig : keys)
        {
            auto key = boost::to_lower_copy(key_orig);

            const auto bracket_pos = key.find('[');
            if (bracket_pos != std::string::npos)
                key.resize(bracket_pos);

            if (key != "host")
                throw Exception{"Unexpected key '" + key_orig + "' in the list of host names/IP addresses (expecting 'host' entries each with a local IPv4/IPv6/hostname)", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

            hosts.push_back(config.getString("listen." + key_orig));
        }
    }

    if (hosts.empty())
        throw Exception{"No local IPs/hostnames are configured to listen from. If in doubt, specify '::1' and/or '127.0.0.1'", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

    if (config.has("port"))
    {
        const auto raw_port = config.getUInt("port");

        if (raw_port > std::numeric_limits<decltype(port)>::max())
            throw Exception{"Value for port is out of range", ErrorCodes::INVALID_CONFIG_PARAMETER};

        port = raw_port;
    }

    if (config.has("try_listen"))
        try_listen = config.getBool("try_listen");
}

void MultiEndpointInterfaceConfigBase::updateConfig(const LegacyGlobalConfigOverrides & global_overrides)
{
    if (!global_overrides.listen_host.empty())
        hosts = global_overrides.listen_host;

    if (global_overrides.listen_try.has_value())
        try_listen = global_overrides.listen_try.value();
}

void MultiEndpointInterfaceConfigBase::updateConfig(const Settings &)
{
}

ProtocolServerAdapter MultiEndpointInterfaceConfigBase::createServerAdapter(IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    ProtocolServerAdapter adapter(name);

    for (const auto & host : hosts)
    {
        try
        {
            createSingleServer(adapter, host, server, pool, async_metrics);
        }
        catch (const Poco::Exception &)
        {
            const auto message = "Listen [" + host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

            if (!try_listen)
                throw Exception{message, ErrorCodes::NETWORK_ERROR};

            LOG_WARNING(&server.logger(), "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider "
                "specifying not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                message
            );
        }
    }

    server.context()->registerServerPort(name, port);

    return adapter;
}

}
