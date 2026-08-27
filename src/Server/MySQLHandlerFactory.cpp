#include <Server/MySQLHandlerFactory.h>

#include <Common/logger_useful.h>
#include <Server/MySQLHandler.h>
#include <base/scope_guard.h>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Common/OpenSSLHelpers.h>
#endif

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Util/Application.h>


namespace DB
{

MySQLHandlerFactory::MySQLHandlerFactory(IServer & server_, bool secure_required_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : server(server_)
    , log(getLogger("MySQLHandlerFactory"))
#if USE_SSL
    , keypair(KeyPair::generateRSA())
#endif
    , secure_required(secure_required_)
    , read_event(read_event_)
    , write_event(write_event_)
{
#if USE_SSL
    try
    {
        Poco::Net::SSLManager::instance().defaultServerContext();
    }
    catch (...)
    {
        LOG_TRACE(log, "Failed to create SSL context. SSL will be disabled. Error: {}", getCurrentExceptionMessage(false));
        ssl_enabled = false;
    }

    /// Reading RSA keys for SHA256 authentication plugin.
    try
    {
        const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();

        String private_key_file_property = "openSSL.server.privateKeyFile";
        String private_key_file = config.getString(private_key_file_property);

        keypair = KeyPair::fromFile(private_key_file);
    }
    catch (...)
    {
        LOG_TRACE(log, "Failed to read RSA key pair from server certificate. Error: {}", getCurrentExceptionMessage(false));
        LOG_TRACE(log, "Generating new RSA key pair.");

        keypair = KeyPair::generateRSA();
    }
#endif
}

Poco::Net::TCPServerConnection * MySQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    uint32_t connection_id = last_connection_id++;
    LOG_TRACE(log, "MySQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
#if USE_SSL
    return new MySQLHandlerSSL(
        server,
        tcp_server,
        socket,
        ssl_enabled,
        secure_required,
        connection_id,
        keypair
    );
#else
    return new MySQLHandler(server, tcp_server, socket, ssl_enabled, secure_required, connection_id);
#endif

}

}
