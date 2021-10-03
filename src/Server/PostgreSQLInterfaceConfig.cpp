#include <Server/PostgreSQLInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/IServer.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <base/logger_useful.h>

#include <Poco/Net/TCPServer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

PostgreSQLInterfaceConfig::PostgreSQLInterfaceConfig(const std::string & name_)
    : TCPInterfaceConfigBase(name_, "postgresql")
{
}

void PostgreSQLInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics *)
{
    if (secure)
        throw Exception{"PostgreSQL compatibility protocol over TLS is not supported", ErrorCodes::NOT_IMPLEMENTED};

    Poco::Net::ServerSocket socket;
    auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(Util::toTimespan(tcp_receive_timeout));
    socket.setSendTimeout(Util::toTimespan(tcp_send_timeout));

    adapter.add(std::make_unique<Poco::Net::TCPServer>(
        new PostgreSQLHandlerFactory(server, *this), pool, socket, new Poco::Net::TCPServerParams));

    LOG_INFO(&server.logger(), "Listening for connections with PostgreSQL compatibility protocol ({}): {}", name, address.toString());
}

std::unique_ptr<PostgreSQLInterfaceConfig> PostgreSQLInterfaceConfig::tryParseLegacyInterface(
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<PostgreSQLInterfaceConfig>(
        false,
        "postgresql_port",
        "LegacyPostgreSQL",
        global_overrides,
        config,
        settings
    );
}

}
