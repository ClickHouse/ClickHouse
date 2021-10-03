#include <Server/MySQLInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/MySQLHandlerFactory.h>
#include <base/logger_useful.h>

#include <Poco/Net/TCPServer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MySQLInterfaceConfig::MySQLInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "mysql")
{
}

void MySQLInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics *)
{
    if (secure)
        throw Exception{"MySQL compatibility protocol over TLS is not supported", ErrorCodes::NOT_IMPLEMENTED};

    Poco::Net::ServerSocket socket;
    auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
    socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

    adapter.add(std::make_unique<Poco::Net::TCPServer>(
        new MySQLHandlerFactory(server, *this), pool, socket, new Poco::Net::TCPServerParams));

    LOG_INFO(&server.logger(), "Listening for connections with MySQL compatibility protocol ({}): {}", name, address.toString());
}

std::unique_ptr<MySQLInterfaceConfig> MySQLInterfaceConfig::tryParseLegacyInterface(
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<MySQLInterfaceConfig>(
        false,
        "mysql_port",
        "LegacyMySQL",
        global_overrides,
        config,
        settings
    );
}

}
