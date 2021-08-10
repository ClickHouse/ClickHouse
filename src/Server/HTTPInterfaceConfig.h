#pragma once

#include <Server/TCPInterfaceConfig.h>

namespace DB
{

/// Base class for server listening interface configs for all HTTP-based protocols.
class HTTPInterfaceConfigBase : public TCPInterfaceConfigBase
{
protected:
    explicit HTTPInterfaceConfigBase(const std::string & name_, const std::string & protocol_);

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_
    ) override;

    virtual void updateConfig(const LegacyGlobalConfigOverrides & global_overrides) override;

    virtual void updateConfig(const Settings & settings) override;

public:
    std::chrono::seconds http_connection_timeout{DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT};
    std::chrono::seconds http_send_timeout{DEFAULT_HTTP_READ_BUFFER_TIMEOUT};
    std::chrono::seconds http_receive_timeout{DEFAULT_HTTP_READ_BUFFER_TIMEOUT};
    std::chrono::seconds http_keep_alive_timeout{10};
};

/// A helper class for storing generic HTTP interface config.
class HTTPInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit HTTPInterfaceConfig(const HTTPInterfaceConfigBase & base);

protected:
    virtual void createSingleServer(
        ProtocolServerAdapter & adapter,
        const std::string & host,
        IServer & server,
        Poco::ThreadPool & pool,
        AsynchronousMetrics * async_metrics
    ) override;
};

}
