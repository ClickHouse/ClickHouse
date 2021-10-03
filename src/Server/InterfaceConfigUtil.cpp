#include <Server/InterfaceConfigUtil.h>
#include <Server/NativeTCPInterfaceConfig.h>
#include <Server/NativeHTTPInterfaceConfig.h>
#include <Server/NativeGRPCInterfaceConfig.h>
#include <Server/InterserverHTTPInterfaceConfig.h>
#include <Server/MySQLInterfaceConfig.h>
#include <Server/PostgreSQLInterfaceConfig.h>
#include <Server/PrometheusInterfaceConfig.h>
#include <Server/KeeperTCPInterfaceConfig.h>
#include <base/logger_useful.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/ServerSocket.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace Util
{

Poco::Net::SocketAddress makeSocketAddress(
    const std::string & host,
    UInt16 port,
    Poco::Logger * logger
)
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

namespace
{

std::unique_ptr<InterfaceConfig> makeInterface(
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

std::unique_ptr<InterfaceConfig> parseInterface(
    const std::string & name,
    const std::string & protocol,
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const ProxyConfigs & proxies,
    const Settings & settings
)
{
    auto interface = makeInterface(name, protocol);
    interface->updateConfig(global_overrides, config, proxies, settings);
    return interface;
}

}

InterfaceConfigs parseInterfaces(
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings,
    const ProxyConfigs & proxies
)
{
    InterfaceConfigs interfaces;

    const auto add_interface = [&] (std::unique_ptr<InterfaceConfig> && interface)
    {
        if (!interface)
            return false;

        if (interfaces.count(boost::to_lower_copy(interface->name)))
            throw Exception("Interface name '" + interface->name + "' already in use", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        interfaces.emplace(interface->name, std::move(interface));
        return true;
    };

    const LegacyGlobalConfigOverrides global_overrides(config);

    // Legacy interface configs:

    const auto has_legacy_plain_native_tcp_config = add_interface(
        NativeTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, global_overrides, config, settings));
    const auto has_legacy_secure_native_tcp_config = add_interface(
        NativeTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, global_overrides, config, settings));

    const auto has_legacy_plain_native_http_config = add_interface(
        NativeHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, global_overrides, config, settings));
    const auto has_legacy_secure_native_http_config = add_interface(
        NativeHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, global_overrides, config, settings));

    const auto has_legacy_native_grpc_config = add_interface(
        NativeGRPCInterfaceConfig::tryParseLegacyInterface(global_overrides, config, settings));

    const auto has_legacy_plain_interserver_http_config = add_interface(
        InterserverHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, global_overrides, config, settings));
    const auto has_legacy_secure_interserver_http_config = add_interface(
        InterserverHTTPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, global_overrides, config, settings));

    const auto has_legacy_mysql_config = add_interface(
        MySQLInterfaceConfig::tryParseLegacyInterface(global_overrides, config, settings));
    const auto has_legacy_postgresql_config = add_interface(
        PostgreSQLInterfaceConfig::tryParseLegacyInterface(global_overrides, config, settings));
    const auto has_legacy_prometheus_config = add_interface(
        PrometheusInterfaceConfig::tryParseLegacyInterface(global_overrides, config, settings));

    const auto has_legacy_plain_keeper_tcp_config = add_interface(
        KeeperTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */false, global_overrides, config, settings));
    const auto has_legacy_secure_keeper_tcp_config = add_interface(
        KeeperTCPInterfaceConfig::tryParseLegacyInterface(/*secure = */true, global_overrides, config, settings));

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
        add_interface(parseInterface(key, protocol, global_overrides, *interface_config, proxies, settings));
    }

    return interfaces;
}

}

}
