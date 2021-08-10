#include <Server/HTTPInterfaceConfig.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

HTTPInterfaceConfigBase::HTTPInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : TCPInterfaceConfigBase(name_, protocol_)
{
}

void HTTPInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs & proxies_
)
{
    TCPInterfaceConfigBase::updateConfig(config, proxies_);

    if (config.has("connection_timeout"))
        http_connection_timeout = std::chrono::seconds{config.getUInt64("connection_timeout")};

    if (config.has("send_timeout"))
        http_send_timeout = std::chrono::seconds{config.getUInt64("send_timeout")};

    if (config.has("receive_timeout"))
        http_receive_timeout = std::chrono::seconds{config.getUInt64("receive_timeout")};

    if (config.has("keep_alive_timeout"))
        http_keep_alive_timeout = std::chrono::seconds{config.getUInt64("keep_alive_timeout")};


    if (config.has("http_connection_timeout"))
        http_connection_timeout = std::chrono::seconds{config.getUInt64("http_connection_timeout")};

    if (config.has("http_send_timeout"))
        http_send_timeout = std::chrono::seconds{config.getUInt64("http_send_timeout")};

    if (config.has("http_receive_timeout"))
        http_receive_timeout = std::chrono::seconds{config.getUInt64("http_receive_timeout")};

    if (config.has("http_keep_alive_timeout"))
        http_keep_alive_timeout = std::chrono::seconds{config.getUInt64("http_keep_alive_timeout")};
}

void HTTPInterfaceConfigBase::updateConfig(const LegacyGlobalConfigOverrides & global_overrides)
{
    TCPInterfaceConfigBase::updateConfig(global_overrides);

    if (global_overrides.connection_timeout.has_value())
        http_connection_timeout = global_overrides.connection_timeout.value();

    if (global_overrides.send_timeout.has_value())
        http_send_timeout = global_overrides.send_timeout.value();

    if (global_overrides.receive_timeout.has_value())
        http_receive_timeout = global_overrides.receive_timeout.value();

    if (global_overrides.keep_alive_timeout.has_value())
        http_keep_alive_timeout = global_overrides.keep_alive_timeout.value();


    if (global_overrides.http_connection_timeout.has_value())
        http_connection_timeout = global_overrides.http_connection_timeout.value();

    if (global_overrides.http_send_timeout.has_value())
        http_send_timeout = global_overrides.http_send_timeout.value();

    if (global_overrides.http_receive_timeout.has_value())
        http_receive_timeout = global_overrides.http_receive_timeout.value();

    if (global_overrides.http_keep_alive_timeout.has_value())
        http_keep_alive_timeout = global_overrides.http_keep_alive_timeout.value();
}

void HTTPInterfaceConfigBase::updateConfig(const Settings & settings)
{
    TCPInterfaceConfigBase::updateConfig(settings);

    if (settings.has("connection_timeout"))
        http_connection_timeout = std::chrono::seconds{settings.connect_timeout};

    if (settings.has("send_timeout"))
        http_send_timeout = std::chrono::seconds{settings.send_timeout};

    if (settings.has("receive_timeout"))
        http_receive_timeout = std::chrono::seconds{settings.receive_timeout};
/*
    if (settings.has("keep_alive_timeout"))
        http_keep_alive_timeout = std::chrono::seconds{settings.keep_alive_timeout};
*/

    if (settings.has("http_connection_timeout"))
        http_connection_timeout = std::chrono::seconds{settings.http_connection_timeout};

    if (settings.has("http_send_timeout"))
        http_send_timeout = std::chrono::seconds{settings.http_send_timeout};

    if (settings.has("http_receive_timeout"))
        http_receive_timeout = std::chrono::seconds{settings.http_receive_timeout};
/*
    if (settings.has("http_keep_alive_timeout"))
        http_keep_alive_timeout = std::chrono::seconds{settings.http_keep_alive_timeout};
*/
}

HTTPInterfaceConfig::HTTPInterfaceConfig(const HTTPInterfaceConfigBase & base)
    : HTTPInterfaceConfigBase(base)
{
}

void HTTPInterfaceConfig::createSingleServer(
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
