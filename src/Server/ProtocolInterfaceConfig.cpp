#include <Server/ProtocolInterfaceConfig.h>
#include <Server/ProxyConfig.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/IServer.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/HTTPHandlerFactory.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Logger.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/TCPServer.h>
#include <Server/HTTP/HTTPServer.h>
#include <Server/MySQLHandlerFactory.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timespan.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/string.hpp>

#if USE_SSL
#   include <Poco/Net/Context.h>
#   include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#if USE_NURAFT
#   include <Server/KeeperTCPHandlerFactory.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

}

namespace
{

template <typename Duration>
Poco::Timespan toTimespan(const Duration & duration)
{
    return Poco::Timespan(std::chrono::duration_cast<std::chrono::microseconds>(duration).count());
}

struct CommonLegacyConfigValues
{
    std::vector<std::string> listen_host;
    bool listen_try = false;
    bool listen_reuse_port = false;
    UInt32 listen_backlog = 64;
    std::chrono::seconds keep_alive_timeout{10};
};

auto getCommonLegacyConfigValues(const Poco::Util::AbstractConfiguration & config)
{
    CommonLegacyConfigValues values;

    values.listen_host = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    values.listen_try = config.getBool("listen_try", false);

    if (values.listen_host.empty())
    {
        values.listen_host.emplace_back("::1");
        values.listen_host.emplace_back("127.0.0.1");
        values.listen_try = true;
    }

    values.listen_reuse_port = config.getBool("listen_reuse_port", false);
    values.listen_backlog = config.getUInt("listen_backlog", 64);

    values.keep_alive_timeout = std::chrono::seconds{config.getUInt("keep_alive_timeout", 10)};

    return values;
}

template <typename InterfaceConfig>
std::unique_ptr<InterfaceConfig> tryParseLegacyTCPInterface(
    const bool secure_,
    const std::string & port_prefix,
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings
)
{
    if (!config.has(port_prefix))
        return {};

    const auto raw_port = config.getUInt(port_prefix);

    if (raw_port > std::numeric_limits<UInt16>::max())
        throw DB::Exception{"Value for port is out of range", DB::ErrorCodes::INVALID_CONFIG_PARAMETER};

    const auto legacy_values = getCommonLegacyConfigValues(config);
    auto interface = std::make_unique<InterfaceConfig>(name);

    interface->hosts = legacy_values.listen_host;
    interface->port = raw_port;
    interface->try_listen = legacy_values.listen_try;
    interface->reuse_port = legacy_values.listen_reuse_port;
    interface->backlog = legacy_values.listen_backlog;
    interface->secure = secure_;
    interface->tcp_connection_timeout = std::chrono::seconds{settings.connect_timeout};
    interface->tcp_send_timeout = std::chrono::seconds{settings.send_timeout};
    interface->tcp_receive_timeout = std::chrono::seconds{settings.receive_timeout};
    interface->tcp_keep_alive_timeout = std::chrono::seconds{settings.tcp_keep_alive_timeout};

    return interface;
}

template <typename InterfaceConfig>
std::unique_ptr<InterfaceConfig> tryParseLegacyHTTPInterface(
    const bool secure_,
    const std::string & port_prefix,
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const DB::Settings & settings
)
{
    if (!config.has(port_prefix))
        return {};

    const auto raw_port = config.getUInt(port_prefix);

    if (raw_port > std::numeric_limits<UInt16>::max())
        throw DB::Exception{"Value for port is out of range", DB::ErrorCodes::INVALID_CONFIG_PARAMETER};

    const auto legacy_values = getCommonLegacyConfigValues(config);
    auto interface = std::make_unique<InterfaceConfig>(name);

    interface->hosts = legacy_values.listen_host;
    interface->port = raw_port;
    interface->try_listen = legacy_values.listen_try;
    interface->reuse_port = legacy_values.listen_reuse_port;
    interface->backlog = legacy_values.listen_backlog;
    interface->secure = secure_;
    interface->http_connection_timeout = std::chrono::seconds{settings.http_connection_timeout};
    interface->http_send_timeout = std::chrono::seconds{settings.http_send_timeout};
    interface->http_receive_timeout = std::chrono::seconds{settings.http_receive_timeout};
    interface->http_keep_alive_timeout = legacy_values.keep_alive_timeout;

    return interface;
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * logger)
{
    Poco::Net::SocketAddress socket_address;
    try
    {
        socket_address = Poco::Net::SocketAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
            || code == EAI_ADDRFAMILY
#endif
        )
        {
            LOG_ERROR(logger, "Cannot resolve host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider specifying IPv4 address instead.",
                host, e.code(), e.message());
        }

        throw;
    }
    return socket_address;
}

Poco::Net::SocketAddress socketBindListen(
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    [[maybe_unused]] bool secure,
    bool reuse_port,
    UInt32 backlog,
    Poco::Logger * logger
)
{
    auto address = makeSocketAddress(host, port, logger);
#if !defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION < 0x01090100
    if (secure)
        /// Bug in old (<1.9.1) poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
        /// https://github.com/pocoproject/poco/pull/2257
        socket.bind(address, /*reuseAddress = */true);
    else
#endif
#if POCO_VERSION < 0x01080000
        socket.bind(address, /*reuseAddress = */true);
#else
        socket.bind(address, /*reuseAddress = */true, reuse_port);
#endif

    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(logger, "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(backlog);

    return address;
}

}

namespace DB
{

ProtocolInterfaceConfig::ProtocolInterfaceConfig(const std::string & name_, const std::string & protocol_)
    : name(name_)
    , protocol(protocol_)
{
}

void ProtocolInterfaceConfig::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    [[maybe_unused]] const Settings & settings,
    [[maybe_unused]] const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
)
{
    if (config.has("protocol") && !boost::iequals(config.getString("protocol"), protocol))
        throw Exception("Cannot modify previously configured protocol", ErrorCodes::INVALID_CONFIG_PARAMETER);
}

MultiEndpointInterfaceConfigBase::MultiEndpointInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : ProtocolInterfaceConfig(name_, protocol_)
{
}

void MultiEndpointInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
)
{
    ProtocolInterfaceConfig::updateConfig(config, settings, proxies_);

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

    return adapter;
}

TCPInterfaceConfigBase::TCPInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : MultiEndpointInterfaceConfigBase(name_, protocol_)
{
}

void TCPInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
)
{
    MultiEndpointInterfaceConfigBase::updateConfig(config, settings, proxies_);

    tcp_connection_timeout = std::chrono::seconds{settings.connect_timeout};
    tcp_send_timeout = std::chrono::seconds{settings.send_timeout};
    tcp_receive_timeout = std::chrono::seconds{settings.receive_timeout};
    tcp_keep_alive_timeout = std::chrono::seconds{settings.tcp_keep_alive_timeout};

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

HTTPInterfaceConfigBase::HTTPInterfaceConfigBase(const std::string & name_, const std::string & protocol_)
    : TCPInterfaceConfigBase(name_, protocol_)
{
}

void HTTPInterfaceConfigBase::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
)
{
    TCPInterfaceConfigBase::updateConfig(config, settings, proxies_);

    http_connection_timeout = std::chrono::seconds{settings.http_connection_timeout};
    http_send_timeout = std::chrono::seconds{settings.http_send_timeout};
    http_receive_timeout = std::chrono::seconds{settings.http_receive_timeout};
//  http_keep_alive_timeout = std::chrono::seconds{settings.http_keep_alive_timeout};

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

NativeTCPInterfaceConfig::NativeTCPInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "native_tcp")
{
}

void NativeTCPInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new TCPHandlerFactory(server, secure, /*proxy protocol = */false), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with secure Native TCP protocol ({}): {}", name, address.toString());
#else
        throw Exception{"Unable to listen for secure Native TCP connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new TCPHandlerFactory(server, secure, /*proxy protocol = */false), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with Native TCP protocol ({}): {}", name, address.toString());
    }
}

std::unique_ptr<NativeTCPInterfaceConfig> NativeTCPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyTCPInterface<NativeTCPInterfaceConfig>(
        secure_,
        (secure_ ? "tcp_port_secure" : "tcp_port"),
        (secure_ ? "LegacySecureNativeTCP" : "LegacyPlainNativeTCP"),
        config,
        settings
    );
}

NativeHTTPInterfaceConfig::NativeHTTPInterfaceConfig(const std::string & name_)
    : HTTPInterfaceConfigBase(name_, "native_http")
{
}

void NativeHTTPInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    if (async_metrics == nullptr)
        throw Exception("AsynchronousMetrics instance not provided", ErrorCodes::LOGICAL_ERROR);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(toTimespan(http_receive_timeout));
    http_params->setKeepAliveTimeout(toTimespan(http_keep_alive_timeout));

    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(http_receive_timeout));
        socket.setSendTimeout(toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "HTTPSHandler-factory"), pool, socket, http_params));

        LOG_INFO(&server.logger(), "Listening for connections with Native HTTPS protocol ({}): https://{}", name, address.toString());
#else
        throw Exception{"Unable to listen for Native HTTPS connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(http_receive_timeout));
        socket.setSendTimeout(toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "HTTPHandler-factory"), pool, socket, http_params));

        LOG_INFO(&server.logger(), "Listening for connections with Native HTTP protocol ({}): http://{}", name, address.toString());
    }
}

std::unique_ptr<NativeHTTPInterfaceConfig> NativeHTTPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyHTTPInterface<NativeHTTPInterfaceConfig>(
        secure_,
        (secure_ ? "https_port" : "http_port"),
        (secure_ ? "LegacyNativeHTTPS" : "LegacyNativeHTTP"),
        config,
        settings
    );
}

NativeGRPCInterfaceConfig::NativeGRPCInterfaceConfig(const std::string & name_)
    : MultiEndpointInterfaceConfigBase(name_, "native_grpc")
{
}

void NativeGRPCInterfaceConfig::updateConfig(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies_
)
{
    MultiEndpointInterfaceConfigBase::updateConfig(config, settings, proxies_);

    if (config.has("allow_direct") && !config.getBool("allow_direct"))
        throw Exception("Since proxies are not supported for Native gRPC connections, allow_direct, if specified, must be set to true", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    if (config.has("allow_proxies"))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("allow_proxies", keys);

        if (!keys.empty())
            throw Exception("Proxies are not supported for Native gRPC connections", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }
}

void NativeGRPCInterfaceConfig::createSingleServer([[maybe_unused]] ProtocolServerAdapter & adapter, [[maybe_unused]] const std::string & host, [[maybe_unused]] IServer & server,[[maybe_unused]] Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
#if USE_GRPC
    auto address = makeSocketAddress(host, port, &server.logger());
    adapter.add(std::make_unique<GRPCServer>(server, address));

    LOG_INFO(&server.logger(), "Listening for connections with Native gRPC protocol ({}): {}", name, address.toString());
#else
    throw Exception("ClickHouse server was built without gRPC library. Cannot use Native gRPC protocol.", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

std::unique_ptr<NativeGRPCInterfaceConfig> NativeGRPCInterfaceConfig::tryParseLegacyInterface(
    const Poco::Util::AbstractConfiguration & config
)
{
    const std::string port_prefix = "grpc_port";

    if (!config.has(port_prefix))
        return {};

    const auto raw_port = config.getUInt(port_prefix);

    if (raw_port > std::numeric_limits<decltype(port)>::max())
        throw Exception{"Value for port is out of range", ErrorCodes::INVALID_CONFIG_PARAMETER};

    const auto legacy_values = getCommonLegacyConfigValues(config);
    auto interface = std::make_unique<NativeGRPCInterfaceConfig>("LegacyGRPC");

    interface->hosts = legacy_values.listen_host;
    interface->port = raw_port;
    interface->try_listen = legacy_values.listen_try;

    return interface;
}

InterserverHTTPInterfaceConfig::InterserverHTTPInterfaceConfig(const std::string & name_)
    : HTTPInterfaceConfigBase(name_, "interserver_http")
{
}

void InterserverHTTPInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    if (async_metrics == nullptr)
        throw Exception("AsynchronousMetrics instance not provided", ErrorCodes::LOGICAL_ERROR);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(toTimespan(http_receive_timeout));
    http_params->setKeepAliveTimeout(toTimespan(http_keep_alive_timeout));

    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(http_receive_timeout));
        socket.setSendTimeout(toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "InterserverIOHTTPSHandler-factory"), pool, socket, http_params));

        LOG_INFO(&server.logger(), "Listening for secure replica communication (interserver) protocol ({}): https://{}", name, address.toString());
#else
        throw Exception{"Unable to listen for secure replica communication (interserver) connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(http_receive_timeout));
        socket.setSendTimeout(toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "InterserverIOHTTPHandler-factory"), pool, socket, http_params));

        LOG_INFO(&server.logger(), "Listening for replica communication (interserver) protocol ({}): http://{}", name, address.toString());
    }
}

std::unique_ptr<InterserverHTTPInterfaceConfig> InterserverHTTPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyHTTPInterface<InterserverHTTPInterfaceConfig>(
        secure_,
        (secure_ ? "interserver_https_port" : "interserver_http_port"),
        (secure_ ? "LegacyInterserverHTTPS" : "LegacyInterserverHTTP"),
        config,
        settings
    );
}

MySQLInterfaceConfig::MySQLInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "mysql")
{
}

void MySQLInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
    Poco::Net::ServerSocket socket;
    auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
    socket.setSendTimeout(toTimespan(tcp_send_timeout));

    adapter.add(std::make_unique<Poco::Net::TCPServer>(
        new MySQLHandlerFactory(server), pool, socket, new Poco::Net::TCPServerParams));

    LOG_INFO(&server.logger(), "Listening for connections with MySQL compatibility protocol ({}): {}", name, address.toString());
}

std::unique_ptr<MySQLInterfaceConfig> MySQLInterfaceConfig::tryParseLegacyInterface(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyTCPInterface<MySQLInterfaceConfig>(
        false,
        "mysql_port",
        "LegacyMySQL",
        config,
        settings
    );
}

PostgreSQLInterfaceConfig::PostgreSQLInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "postgresql")
{
}

void PostgreSQLInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
    Poco::Net::ServerSocket socket;
    auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
    socket.setSendTimeout(toTimespan(tcp_send_timeout));

    adapter.add(std::make_unique<Poco::Net::TCPServer>(
        new PostgreSQLHandlerFactory(server), pool, socket, new Poco::Net::TCPServerParams));

    LOG_INFO(&server.logger(), "Listening for connections with PostgreSQL compatibility protocol ({}): {}", name, address.toString());
}

std::unique_ptr<PostgreSQLInterfaceConfig> PostgreSQLInterfaceConfig::tryParseLegacyInterface(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyTCPInterface<PostgreSQLInterfaceConfig>(
        false,
        "postgresql_port",
        "LegacyPostgreSQL",
        config,
        settings
    );
}

PrometheusInterfaceConfig::PrometheusInterfaceConfig(const std::string & name_)
    : HTTPInterfaceConfigBase(name_, "prometheus")
{
}

void PrometheusInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    if (async_metrics == nullptr)
        throw Exception("AsynchronousMetrics instance not provided", ErrorCodes::LOGICAL_ERROR);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(toTimespan(http_receive_timeout));
    http_params->setKeepAliveTimeout(toTimespan(http_keep_alive_timeout));

    Poco::Net::ServerSocket socket;
    auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(toTimespan(http_receive_timeout));
    socket.setSendTimeout(toTimespan(http_send_timeout));

    adapter.add(std::make_unique<HTTPServer>(
        server.context(), createHandlerFactory(server, *async_metrics, "PrometheusHandler-factory"), pool, socket, http_params));

    LOG_INFO(&server.logger(), "Listening for connections with Prometheus protocol ({}): http://{}", name, address.toString());
}

std::unique_ptr<PrometheusInterfaceConfig> PrometheusInterfaceConfig::tryParseLegacyInterface(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyHTTPInterface<PrometheusInterfaceConfig>(
        false,
        "prometheus.port",
        "LegacyPrometheus",
        config,
        settings
    );
}

KeeperTCPInterfaceConfig::KeeperTCPInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "keeper_tcp")
{
}

void KeeperTCPInterfaceConfig::createSingleServer([[maybe_unused]] ProtocolServerAdapter & adapter, [[maybe_unused]] const std::string & host, [[maybe_unused]] IServer & server, [[maybe_unused]] Poco::ThreadPool & pool, [[maybe_unused]] AsynchronousMetrics * async_metrics)
{
#if USE_NURAFT
    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new KeeperTCPHandlerFactory(server, secure), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with secure Keeper TCP protocol ({}): {}", name, address.toString());
#else
        throw Exception{"Unable to listen for secure Keeper TCP connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new KeeperTCPHandlerFactory(server, secure), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with Keeper TCP protocol ({}): {}", name, address.toString());
    }
#else
    throw Exception("ClickHouse server was built without NuRaft library. Cannot use internal coordination.", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

std::unique_ptr<KeeperTCPInterfaceConfig> KeeperTCPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return tryParseLegacyTCPInterface<KeeperTCPInterfaceConfig>(
        secure_,
        (secure_ ? "keeper_server.tcp_port_secure" : "keeper_server.tcp_port"),
        (secure_ ? "LegacyЫусгкуKeeperTCP" : "LegacyPlainKeeperTCP"),
        config,
        settings
    );
}

namespace Util
{

std::unique_ptr<ProtocolInterfaceConfig> makeInterface(
    const std::string & name,
    const std::string & protocol
)
{
    if (boost::iequals(protocol, "native_tcp")) return std::make_unique<NativeTCPInterfaceConfig>(name);
    if (boost::iequals(protocol, "native_http")) return std::make_unique<NativeHTTPInterfaceConfig>(name);
    if (boost::iequals(protocol, "native_grpc")) return std::make_unique<NativeGRPCInterfaceConfig>(name);
    if (boost::iequals(protocol, "interserver_http")) return std::make_unique<InterserverHTTPInterfaceConfig>(name);
    if (boost::iequals(protocol, "mysql")) return std::make_unique<MySQLInterfaceConfig>(name);
    if (boost::iequals(protocol, "postgresql")) return std::make_unique<PostgreSQLInterfaceConfig>(name);
    if (boost::iequals(protocol, "prometheus")) return std::make_unique<PrometheusInterfaceConfig>(name);
    if (boost::iequals(protocol, "keeper_tcp")) return std::make_unique<KeeperTCPInterfaceConfig>(name);

    throw Exception("Unknown interface protocol '" + protocol + "'", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
}

std::unique_ptr<ProtocolInterfaceConfig> parseInterface(
    const std::string & name,
    const std::string & protocol,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies
)
{
    auto interface = makeInterface(name, protocol);
    interface->updateConfig(config, settings, proxies);
    return interface;
}

std::map<std::string, std::unique_ptr<ProtocolInterfaceConfig>> parseInterfaces(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const std::map<std::string, std::unique_ptr<ProxyConfig>> & proxies
)
{
    std::map<std::string, std::unique_ptr<ProtocolInterfaceConfig>> interfaces;

    const auto add_interface = [&] (std::unique_ptr<ProtocolInterfaceConfig> && interface)
    {
        if (!interface)
            return false;

        if (interfaces.count(boost::to_lower_copy(interface->name)))
            throw Exception("Interface name '" + interface->name + "' already in use", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        interfaces.emplace(interface->name, std::move(interface));
        return true;
    };

    // Legacy interface configs:

    const auto has_legacy_plain_native_tcp_config = add_interface(NativeTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, config, settings));
    const auto has_legacy_secure_native_tcp_config = add_interface(NativeTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, config, settings));

    const auto has_legacy_plain_native_http_config = add_interface(NativeHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, config, settings));
    const auto has_legacy_secure_native_http_config = add_interface(NativeHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, config, settings));

    const auto has_legacy_native_grpc_config = add_interface(NativeGRPCInterfaceConfig::tryParseLegacyInterface(config));

    const auto has_legacy_plain_interserver_http_config = add_interface(InterserverHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, config, settings));
    const auto has_legacy_secure_interserver_http_config = add_interface(InterserverHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, config, settings));

    const auto has_legacy_mysql_config = add_interface(MySQLInterfaceConfig::tryParseLegacyInterface(config, settings));
    const auto has_legacy_postgresql_config = add_interface(PostgreSQLInterfaceConfig::tryParseLegacyInterface(config, settings));
    const auto has_legacy_prometheus_config = add_interface(PrometheusInterfaceConfig::tryParseLegacyInterface(config, settings));

    const auto has_legacy_plain_keeper_tcp_config = add_interface(KeeperTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, config, settings));
    const auto has_legacy_secure_keeper_tcp_config = add_interface(KeeperTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, config, settings));

    // Regular interface configs:

    if (!config.has("interfaces"))
        return interfaces;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("interfaces", keys);

    for (const auto & key : keys)
    {
        const auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos)
            throw Exception("Interface name '" + key.substr(0, bracket_pos) + "' already in use", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        const auto prefix = "interfaces." + key;

        if (!config.has(prefix + ".protocol"))
            throw Exception("Missing protocol for " + key + " interface", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        const auto protocol = config.getString(prefix + ".protocol");

        if (
            (boost::iequals(protocol, "native_tcp") && (has_legacy_plain_native_tcp_config || has_legacy_secure_native_tcp_config)) ||
            (boost::iequals(protocol, "native_http") && (has_legacy_plain_native_http_config || has_legacy_secure_native_http_config)) ||
            (boost::iequals(protocol, "native_grpc") && has_legacy_native_grpc_config) ||
            (boost::iequals(protocol, "interserver_http") && (has_legacy_plain_interserver_http_config || has_legacy_secure_interserver_http_config)) ||
            (boost::iequals(protocol, "mysql") && has_legacy_mysql_config) ||
            (boost::iequals(protocol, "postgresql") && has_legacy_postgresql_config) ||
            (boost::iequals(protocol, "prometheus") && has_legacy_prometheus_config) ||
            (boost::iequals(protocol, "keeper_tcp") && (has_legacy_plain_keeper_tcp_config || has_legacy_secure_keeper_tcp_config))
        )
        {
            throw Exception("Interface " + key + " uses " + protocol + " protocol which has already been configured using the legacy syntax", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        }

        Poco::AutoPtr<Poco::Util::AbstractConfiguration> interface_config(
            const_cast<Poco::Util::AbstractConfiguration &>(config).createView(prefix));
        add_interface(parseInterface(key, protocol, *interface_config, settings, proxies));
    }

    return interfaces;
}

}

}
