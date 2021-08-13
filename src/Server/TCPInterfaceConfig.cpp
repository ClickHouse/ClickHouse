#include <Server/TCPInterfaceConfig.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

TCPInterfaceConfigBase::TCPInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : MultiEndpointInterfaceConfigBase(name_, protocol_)
{
}

TCPInterfaceConfigBase::TCPInterfaceConfigBase(const TCPInterfaceConfigBase& other)
    : MultiEndpointInterfaceConfigBase(other)
    , reuse_port(other.reuse_port)
    , backlog(other.backlog)
    , secure(other.secure)
    , allow_direct(other.allow_direct)
    , proxies(Util::clone(other.proxies))
    , tcp_connection_timeout(other.tcp_connection_timeout)
    , tcp_send_timeout(other.tcp_send_timeout)
    , tcp_receive_timeout(other.tcp_receive_timeout)
    , tcp_keep_alive_timeout(other.tcp_keep_alive_timeout)
{
}

void TCPInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs & proxies_
)
{
    MultiEndpointInterfaceConfigBase::updateConfig(config, proxies_);

    if (config.has("reuse_port"))
        reuse_port = config.getBool("reuse_port");

    if (config.has("backlog"))
        backlog = config.getUInt("backlog");

    if (config.has("enable_tls"))
        secure = config.getBool("enable_tls");


    if (config.has("connection_timeout"))
        tcp_connection_timeout = std::chrono::seconds{config.getUInt64("connection_timeout")};

    if (config.has("send_timeout"))
        tcp_send_timeout = std::chrono::seconds{config.getUInt64("send_timeout")};

    if (config.has("receive_timeout"))
        tcp_receive_timeout = std::chrono::seconds{config.getUInt64("receive_timeout")};

    if (config.has("keep_alive_timeout"))
        tcp_keep_alive_timeout = std::chrono::seconds{config.getUInt64("keep_alive_timeout")};


    if (config.has("tcp_connection_timeout"))
        tcp_connection_timeout = std::chrono::seconds{config.getUInt64("tcp_connection_timeout")};

    if (config.has("tcp_send_timeout"))
        tcp_send_timeout = std::chrono::seconds{config.getUInt64("tcp_send_timeout")};

    if (config.has("tcp_receive_timeout"))
        tcp_receive_timeout = std::chrono::seconds{config.getUInt64("tcp_receive_timeout")};

    if (config.has("tcp_keep_alive_timeout"))
        tcp_keep_alive_timeout = std::chrono::seconds{config.getUInt64("tcp_keep_alive_timeout")};


    if (config.has("allow_direct"))
        allow_direct = config.getBool("allow_direct");

    if (config.has("allow_proxies"))
    {
        proxies.clear();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("allow_proxies", keys);

        for (const auto & key : keys)
        {
            const auto it = proxies_.find(key);

            if (it == proxies_.end())
                throw Exception{"Proxy with name '" + key + "' is not configured", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

            const auto & proxy_base = it->second;
            auto & proxy = proxies[key];

            proxy = proxy_base->clone();

            Poco::AutoPtr<Poco::Util::AbstractConfiguration> proxy_config(
                const_cast<Poco::Util::AbstractConfiguration &>(config).createView("allow_proxies." + key));
            proxy->updateConfig(*proxy_config);
        }
    }

    if (!allow_direct && proxies.empty())
        throw Exception{"No allowed proxies are set for the interface, and the direct connections are not allowed either", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};
}

void TCPInterfaceConfigBase::updateConfig(const LegacyGlobalConfigOverrides & global_overrides)
{
    MultiEndpointInterfaceConfigBase::updateConfig(global_overrides);

    if (global_overrides.listen_reuse_port.has_value())
        reuse_port = global_overrides.listen_reuse_port.value();

    if (global_overrides.listen_backlog.has_value())
        backlog = global_overrides.listen_backlog.value();


    if (global_overrides.connection_timeout.has_value())
        tcp_connection_timeout = global_overrides.connection_timeout.value();

    if (global_overrides.send_timeout.has_value())
        tcp_send_timeout = global_overrides.send_timeout.value();

    if (global_overrides.receive_timeout.has_value())
        tcp_receive_timeout = global_overrides.receive_timeout.value();

    if (global_overrides.keep_alive_timeout.has_value())
        tcp_keep_alive_timeout = global_overrides.keep_alive_timeout.value();


    if (global_overrides.tcp_connection_timeout.has_value())
        tcp_connection_timeout = global_overrides.tcp_connection_timeout.value();

    if (global_overrides.tcp_send_timeout.has_value())
        tcp_send_timeout = global_overrides.tcp_send_timeout.value();

    if (global_overrides.tcp_receive_timeout.has_value())
        tcp_receive_timeout = global_overrides.tcp_receive_timeout.value();

    if (global_overrides.tcp_keep_alive_timeout.has_value())
        tcp_keep_alive_timeout = global_overrides.tcp_keep_alive_timeout.value();
}

void TCPInterfaceConfigBase::updateConfig(const Settings & settings)
{
    MultiEndpointInterfaceConfigBase::updateConfig(settings);

    if (settings.has("connection_timeout"))
        tcp_connection_timeout = std::chrono::seconds{settings.connect_timeout};

    if (settings.has("send_timeout"))
        tcp_send_timeout = std::chrono::seconds{settings.send_timeout};

    if (settings.has("receive_timeout"))
        tcp_receive_timeout = std::chrono::seconds{settings.receive_timeout};
/*
    if (settings.has("keep_alive_timeout"))
        tcp_keep_alive_timeout = std::chrono::seconds{settings.keep_alive_timeout};


    if (settings.has("tcp_connection_timeout"))
        tcp_connection_timeout = std::chrono::seconds{settings.tcp_connection_timeout};

    if (settings.has("tcp_send_timeout"))
        tcp_send_timeout = std::chrono::seconds{settings.tcp_send_timeout};

    if (settings.has("tcp_receive_timeout"))
        tcp_receive_timeout = std::chrono::seconds{settings.tcp_receive_timeout};
*/
    if (settings.has("tcp_keep_alive_timeout"))
        tcp_keep_alive_timeout = std::chrono::seconds{settings.tcp_keep_alive_timeout};
}

TCPInterfaceConfig::TCPInterfaceConfig(const TCPInterfaceConfigBase & base)
    : TCPInterfaceConfigBase(base)
{
}

void TCPInterfaceConfig::createSingleServer(
    ProtocolServerAdapter &,
    const std::string &,
    IServer &,
    Poco::ThreadPool &,
    AsynchronousMetrics *
)
{
    throw Exception{"Not supposed to be called", ErrorCodes::LOGICAL_ERROR};
}

}
