#include <Server/KeeperTCPInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/ProtocolServerAdapter.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>

#if USE_NURAFT
#   include <Server/KeeperTCPHandlerFactory.h>
#endif

#include <Poco/Net/TCPServer.h>

#if USE_SSL
#   include <Poco/Net/Context.h>
#   include <Poco/Net/SecureServerSocket.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

KeeperTCPInterfaceConfig::KeeperTCPInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "keeper_tcp")
{
}

void KeeperTCPInterfaceConfig::createSingleServer([[maybe_unused]] ProtocolServerAdapter & adapter, [[maybe_unused]] const std::string & host, [[maybe_unused]] IServer & server, [[maybe_unused]] Poco::ThreadPool & pool, AsynchronousMetrics *)
{
#if USE_NURAFT
    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new KeeperTCPHandlerFactory(server, secure, *this), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with secure Keeper TCP protocol ({}): {}", name, address.toString());
#else
        throw Exception{"Unable to listen for secure Keeper TCP connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new KeeperTCPHandlerFactory(server, secure, *this), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with Keeper TCP protocol ({}): {}", name, address.toString());
    }
#else
    throw Exception("ClickHouse server was built without NuRaft library. Cannot use internal coordination.", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

std::unique_ptr<KeeperTCPInterfaceConfig> KeeperTCPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<KeeperTCPInterfaceConfig>(
        secure_,
        (secure_ ? "keeper_server.tcp_port_secure" : "keeper_server.tcp_port"),
        (secure_ ? "LegacySecureKeeperTCP" : "LegacyPlainKeeperTCP"),
        global_overrides,
        config,
        settings
    );
}

}
