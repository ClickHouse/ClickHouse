#include <Server/NativeTCPInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>

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

NativeTCPInterfaceConfig::NativeTCPInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "native_tcp")
{
}

void NativeTCPInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics *)
{
    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new TCPHandlerFactory(server, secure, *this), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with secure Native TCP protocol ({}): {}", name, address.toString());
#else
        throw Exception{"Unable to listen for secure Native TCP connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

        adapter.add(std::make_unique<Poco::Net::TCPServer>(
            new TCPHandlerFactory(server, secure, *this), pool, socket, new Poco::Net::TCPServerParams));

        LOG_INFO(&server.logger(), "Listening for connections with Native TCP protocol ({}): {}", name, address.toString());
    }
}

std::unique_ptr<NativeTCPInterfaceConfig> NativeTCPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<NativeTCPInterfaceConfig>(
        secure_,
        (secure_ ? "tcp_port_secure" : "tcp_port"),
        (secure_ ? "LegacySecureNativeTCP" : "LegacyPlainNativeTCP"),
        global_overrides,
        config,
        settings
    );
}

}
