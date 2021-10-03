#pragma once

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#   include "config_core.h"
#endif

#include <Core/Settings.h>
#include <base/types.h>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace Poco { class ThreadPool; }
namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class AsynchronousMetrics;
class IServer;
class ProtocolServerAdapter;

class ProxyConfig;
using ProxyConfigs = std::map<std::string, std::unique_ptr<ProxyConfig>>;

class InterfaceConfig;
using InterfaceConfigs = std::map<std::string, std::unique_ptr<InterfaceConfig>>;

class LegacyGlobalConfigOverrides
{
public:
    explicit LegacyGlobalConfigOverrides(const Poco::Util::AbstractConfiguration & config);

public:
    std::vector<std::string> listen_host;
    std::optional<bool> listen_try;
    std::optional<bool> listen_reuse_port;
    std::optional<UInt32> listen_backlog;

    std::optional<std::chrono::seconds> connection_timeout;
    std::optional<std::chrono::seconds> send_timeout;
    std::optional<std::chrono::seconds> receive_timeout;
    std::optional<std::chrono::seconds> keep_alive_timeout;

    std::optional<std::chrono::seconds> tcp_connection_timeout;
    std::optional<std::chrono::seconds> tcp_send_timeout;
    std::optional<std::chrono::seconds> tcp_receive_timeout;
    std::optional<std::chrono::seconds> tcp_keep_alive_timeout;

    std::optional<std::chrono::seconds> http_connection_timeout;
    std::optional<std::chrono::seconds> http_send_timeout;
    std::optional<std::chrono::seconds> http_receive_timeout;
    std::optional<std::chrono::seconds> http_keep_alive_timeout;
};

/// Base class for server listening interface configs for all protocols.
class InterfaceConfig
{
public:
    explicit InterfaceConfig(const std::string & name_, const std::string & protocol_);
    virtual ~InterfaceConfig() = default;

    void updateConfig(
        const LegacyGlobalConfigOverrides & global_overrides,
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_,
        const Settings & settings
    );

    virtual ProtocolServerAdapter createServerAdapter(
        IServer & server,
        Poco::ThreadPool & pool,
        AsynchronousMetrics * async_metrics
    ) = 0;

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_
    ) = 0;

    virtual void updateConfig(const LegacyGlobalConfigOverrides & global_overrides) = 0;

    virtual void updateConfig(const Settings & settings) = 0;

public:
    const std::string name;
    const std::string protocol;
};

/// Base class for server listening interface configs for all milti-endpoint protocols.
class MultiEndpointInterfaceConfigBase : public InterfaceConfig
{
protected:
    explicit MultiEndpointInterfaceConfigBase(const std::string & name_, const std::string & protocol_);

public:
    virtual ProtocolServerAdapter createServerAdapter(
        IServer & server,
        Poco::ThreadPool & pool,
        AsynchronousMetrics * async_metrics
    ) final override;

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_
    ) override;

    virtual void updateConfig(const LegacyGlobalConfigOverrides & global_overrides) override;

    virtual void updateConfig(const Settings & settings) override;

protected:
    virtual void createSingleServer(
        ProtocolServerAdapter & adapter,
        const std::string & host,
        IServer & server,
        Poco::ThreadPool & pool,
        AsynchronousMetrics * async_metrics
    ) = 0;

public:
    std::vector<std::string> hosts;
    UInt16 port = 0;
    bool try_listen = false;
};

}
