#pragma once

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#   include "config_core.h"
#endif

#include <common/types.h>
#include <Core/Settings.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace Poco { class ThreadPool; }
namespace Poco::Net { class TCPServer; }
namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class AsynchronousMetrics;
class IServer;
class ProtocolServerAdapter;
class ProxyConfig;

/// Base class for server listening interface configs for all protocols.
class ProtocolInterfaceConfig
{
public:
    explicit ProtocolInterfaceConfig(const std::string & name_, const std::string & protocol_);
    virtual ~ProtocolInterfaceConfig() = default;

    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
    );

    virtual ProtocolServerAdapter createServerAdapter(IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) = 0;

public:
    const std::string name;
    const std::string protocol;
};

/// Base class for server listening interface configs for all milti-endpoint protocols.
class MultiEndpointInterfaceConfigBase : public ProtocolInterfaceConfig
{
protected:
    explicit MultiEndpointInterfaceConfigBase(const std::string & name_, const std::string & protocol_);

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
    ) override;

    virtual ProtocolServerAdapter createServerAdapter(IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) final override;

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) = 0;

public:
    std::vector<std::string> hosts;
    UInt16 port = 0;
    bool try_listen = false;
};

/// Base class for server listening interface configs for all TCP-based protocols.
class TCPInterfaceConfigBase : public MultiEndpointInterfaceConfigBase
{
protected:
    explicit TCPInterfaceConfigBase(const std::string & name_, const std::string & protocol_);

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
    ) override;

public:
    bool reuse_port = false;
    UInt32 backlog = 64;
    bool secure = false;

    bool allow_direct = true;
    std::map<std::string, std::unique_ptr<ProxyConfig>> proxies;

    std::chrono::seconds tcp_connection_timeout{DBMS_DEFAULT_CONNECT_TIMEOUT_SEC};
    std::chrono::seconds tcp_send_timeout{DBMS_DEFAULT_SEND_TIMEOUT_SEC};
    std::chrono::seconds tcp_receive_timeout{DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC};
    std::chrono::seconds tcp_keep_alive_timeout{0};
};

/// Base class for server listening interface configs for all HTTP-based protocols.
class HTTPInterfaceConfigBase : public TCPInterfaceConfigBase
{
protected:
    explicit HTTPInterfaceConfigBase(const std::string & name_, const std::string & protocol_);

public:
    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
    ) override;

public:
    std::chrono::seconds http_connection_timeout{DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT};
    std::chrono::seconds http_send_timeout{DEFAULT_HTTP_READ_BUFFER_TIMEOUT};
    std::chrono::seconds http_receive_timeout{DEFAULT_HTTP_READ_BUFFER_TIMEOUT};
    std::chrono::seconds http_keep_alive_timeout{10};
};

/// Class for server listening interface configs for Native TCP protocol.
class NativeTCPInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit NativeTCPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<NativeTCPInterfaceConfig> tryParseLegacyInterface(
        const bool secure_,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for Native HTTP protocol.
class NativeHTTPInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit NativeHTTPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<NativeHTTPInterfaceConfig> tryParseLegacyInterface(
        const bool secure_,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for Native gRPC protocol.
class NativeGRPCInterfaceConfig final : public MultiEndpointInterfaceConfigBase
{
public:
    explicit NativeGRPCInterfaceConfig(const std::string & name_);

    virtual void updateConfig(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings,
        const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
    ) override;

    static std::unique_ptr<NativeGRPCInterfaceConfig> tryParseLegacyInterface(
        const Poco::Util::AbstractConfiguration & config
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for Interserver HTTP protocol.
class InterserverHTTPInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit InterserverHTTPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<InterserverHTTPInterfaceConfig> tryParseLegacyInterface(
        const bool secure_,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for MySQL compatibility protocol.
class MySQLInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit MySQLInterfaceConfig(const std::string & name_);

    static std::unique_ptr<MySQLInterfaceConfig> tryParseLegacyInterface(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for PostgreSQL compatibility protocol.
class PostgreSQLInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit PostgreSQLInterfaceConfig(const std::string & name_);

    static std::unique_ptr<PostgreSQLInterfaceConfig> tryParseLegacyInterface(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for Prometheus protocol.
class PrometheusInterfaceConfig final : public HTTPInterfaceConfigBase
{
public:
    explicit PrometheusInterfaceConfig(const std::string & name_);

    static std::unique_ptr<PrometheusInterfaceConfig> tryParseLegacyInterface(
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

/// Class for server listening interface configs for Keeper TCP protocol.
class KeeperTCPInterfaceConfig final : public TCPInterfaceConfigBase
{
public:
    explicit KeeperTCPInterfaceConfig(const std::string & name_);

    static std::unique_ptr<KeeperTCPInterfaceConfig> tryParseLegacyInterface(
        const bool secure_,
        const Poco::Util::AbstractConfiguration & config,
        const Settings & settings
    );

protected:
    virtual void createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics) override;
};

namespace Util
{

std::map<std::string, std::unique_ptr<ProtocolInterfaceConfig>> parseInterfaces(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies
);

}

}
