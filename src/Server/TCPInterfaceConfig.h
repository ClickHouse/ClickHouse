#pragma once

#include <Server/InterfaceConfig.h>
#include <Server/ProxyConfig.h>

#include <chrono>

namespace DB
{

/// Base class for server listening interface configs for all TCP-based protocols.
class TCPInterfaceConfigBase : public MultiEndpointInterfaceConfigBase
{
protected:
    explicit TCPInterfaceConfigBase(const std::string & name_, const std::string & protocol_);
    TCPInterfaceConfigBase(const TCPInterfaceConfigBase& other);

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const ProxyConfigs & proxies_
    ) override;

    virtual void updateConfig(const LegacyGlobalConfigOverrides & global_overrides) override;

    virtual void updateConfig(const Settings & settings) override;

public:
    bool reuse_port = false;
    UInt32 backlog = 4096;
    bool secure = false;

    bool allow_direct = true;
    ProxyConfigs proxies;

    std::chrono::seconds tcp_connection_timeout{DBMS_DEFAULT_CONNECT_TIMEOUT_SEC};
    std::chrono::seconds tcp_send_timeout{DBMS_DEFAULT_SEND_TIMEOUT_SEC};
    std::chrono::seconds tcp_receive_timeout{DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC};
    std::chrono::seconds tcp_keep_alive_timeout{0};
};

/// A helper class for storing a generic TCP interface config.
class TCPInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit TCPInterfaceConfig(const TCPInterfaceConfigBase & base);

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
