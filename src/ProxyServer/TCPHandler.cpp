#include "TCPHandler.h"

#include <Poco/Net/NetException.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Core/Defines.h>
#include <Core/Protocol.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <ProxyServer/Rules.h>
#include "Common/logger_useful.h"
#include <Common/DNSResolver.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

namespace Proxy
{

TCPHandler::TCPHandler(IProxyServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RouterPtr router_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getLogger("TCPHandler"))
    , router(router_)
{
}

TCPHandler::~TCPHandler() = default;

void TCPHandler::run()
{
    try
    {
        runImpl();
        LOG_DEBUG(log, "Done processing connection.");
    }
    catch (...)
    {
        DB::tryLogCurrentException(log, "TCPHandler");
        throw;
    }
}

void TCPHandler::runImpl()
{
    setThreadName("TCPHandler");

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<DB::ReadBufferFromPocoSocketChunked>(socket());

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    out = std::make_shared<DB::AutoCanceledWriteBuffer<DB::WriteBufferFromPocoSocketChunked>>(socket());

    try
    {
        receiveHello();
        if (rejected)
        {
            try
            {
                socket().shutdown();
            }
            catch (...)
            {
                LOG_WARNING(log, "Error shutting down client connection");
            }
            try
            {
                out->finalize();
            }
            catch (...)
            {
                LOG_ERROR(log, "Error finalizing client output buffer");
            }        
            return;
        }
        startRedirection();
    }
    catch (const DB::Exception & _)
    {
        // if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        // {
        //     LOG_DEBUG(log, "Client has connected to wrong port.");
        //     return;
        // }

        // if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        // {
        //     LOG_INFO(log, "Client has gone away.");
        //     return;
        // }

        // try
        // {
        //     /// We try to send error information to the client.
        //     sendException(e, send_exception_with_stack_trace);
        // }
        // catch (...)
        // {
        //     tryLogCurrentException(__PRETTY_FUNCTION__);
        // }

        throw;
    }
}

namespace
{

std::string formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(const Poco::Util::AbstractConfiguration & config)
{
    std::string result = fmt::format(
        "HTTP/1.0 400 Bad Request\r\n\r\n"
        "Port {} is for clickhouse-client program\r\n",
        config.getString("tcp_port"));

    if (config.has("http_port"))
    {
        result += fmt::format("You must use port {} for HTTP.\r\n", config.getString("http_port"));
    }

    return result;
}

}

void TCPHandler::receiveHello()
{
    UInt64 packet_type = 0;
    String user;
    String password;
    String default_database;
    String hostname;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt32 client_tcp_protocol_version = 0;

    readVarUInt(packet_type, *in);

    if (packet_type != DB::Protocol::Client::Hello)
    {
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString(formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(server.config()), *out);
            out->next();
            throw DB::Exception(DB::ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT, "Client has connected to wrong port");
        }
        throw DB::Exception(
            DB::ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client (expected Hello, got {})", packet_type);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    readVarUInt(client_tcp_protocol_version, *in);
    readStringBinary(default_database, *in);
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    if (user.empty())
        throw DB::Exception(DB::ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client (no user in Hello package)");

    LOG_DEBUG(
        log,
        "Connected {} version {}.{}.{}, revision: {}{}{}.",
        client_name,
        client_version_major,
        client_version_minor,
        client_version_patch,
        client_tcp_protocol_version,
        (!default_database.empty() ? ", database: " + default_database : ""),
        (!user.empty() ? ", user: " + user : ""));

    auto action = router->route(user, hostname, default_database);

    switch (action.type)
    {
        case RuleActionType::Reject: {
            rejected = true;
            LOG_DEBUG(log, "Rejecting connection via router rules");
            return;
        }
        case RuleActionType::Route: {
            rejected = false;
            client_connection = ClientConnection{
                .user = std::move(user),
                .password = std::move(password),
                .default_database = std::move(default_database),
                .hostname = std::move(hostname),
                .client_name = std::move(client_name),
                .client_version_major = client_version_major,
                .client_version_minor = client_version_minor,
                .client_version_patch = client_version_patch,
                .client_tcp_protocol_version = client_tcp_protocol_version};

            assigned_server = std::move(*action.route_to);

            LOG_DEBUG(log, "Redirecting connection to {}:{}", assigned_server.host, assigned_server.tcp_port);

            connect();
            // sendHello();
        }
    }
}

void TCPHandler::connect()
{
    auto addresses = DB::DNSResolver::instance().resolveAddressList(assigned_server.host, assigned_server.tcp_port);
    const auto connection_timeout = DB::DBMS_DEFAULT_CONNECT_TIMEOUT_SEC; // TODO: move to config

    for (auto it = addresses.begin(); it != addresses.end();)
    {
        mid_socket = std::make_unique<Poco::Net::StreamSocket>();

        try
        {
            mid_socket->connect(*it, connection_timeout);
            break;
        }
        catch (DB::NetException &)
        {
            if (++it == addresses.end())
                throw;
            continue;
        }
        catch (Poco::Net::NetException &)
        {
            if (++it == addresses.end())
                throw;
            continue;
        }
        catch (Poco::TimeoutException &)
        {
            if (++it == addresses.end())
                throw;
            continue;
        }
    }

    mid_socket->setReceiveTimeout(DB::DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC); // TODO: move to config
    mid_socket->setSendTimeout(DB::DBMS_DEFAULT_SEND_TIMEOUT_SEC); // TODO: move to config
    mid_socket->setNoDelay(true);

    // TODO keepalive ?

    mid_in = std::make_shared<DB::ReadBufferFromPocoSocketChunked>(*mid_socket);
    // in->setAsyncCallback(async_callback);

    mid_out = std::make_shared<DB::AutoCanceledWriteBuffer<DB::WriteBufferFromPocoSocketChunked>>(*mid_socket);
    // out->setAsyncCallback(async_callback);

    sendHello();
}

void TCPHandler::sendHello()
{
    auto has_control_character = [](const std::string & s)
    {
        for (auto c : s)
            if (isControlASCII(c))
                return true;
        return false;
    };

    if (has_control_character(client_connection.default_database) || has_control_character(client_connection.user)
        || has_control_character(client_connection.password))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Parameters 'default_database', 'user' and 'password' must not contain ASCII control characters");

    writeVarUInt(DB::Protocol::Client::Hello, *mid_out);
    writeStringBinary(client_connection.client_name, *mid_out);
    writeVarUInt(client_connection.client_version_major, *mid_out);
    writeVarUInt(client_connection.client_version_minor, *mid_out);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    writeVarUInt(client_connection.client_tcp_protocol_version, *mid_out);
    writeStringBinary(client_connection.default_database, *mid_out);

    writeStringBinary(client_connection.user, *mid_out);
    writeStringBinary(client_connection.password, *mid_out);

    mid_out->next();
}

void TCPHandler::startRedirection()
{
    constexpr size_t buffer_size = 64 * 1024;
    char buffer[buffer_size];

    Poco::Net::Socket::SocketList read_list;
    read_list.push_back(socket());
    read_list.push_back(*mid_socket);

    bool client_active = true;
    bool server_active = true;

    LOG_DEBUG(log, "Starting proxy redirection between client and server {}:{}", assigned_server.host, assigned_server.tcp_port);

    try
    {
        while (client_active && server_active)
        {
            Poco::Net::Socket::SocketList read_ready;
            Poco::Net::Socket::SocketList write_ready;
            Poco::Net::Socket::SocketList except_ready;

            read_ready = read_list;

            Poco::Net::Socket::select(read_ready, write_ready, except_ready, Poco::Timespan(1, 0));

            if (std::find(read_ready.begin(), read_ready.end(), socket()) != read_ready.end())
            {
                try
                {
                    int bytes_read = socket().receiveBytes(buffer, buffer_size);
                    if (bytes_read > 0)
                    {
                        mid_socket->sendBytes(buffer, bytes_read);
                    }
                    else if (bytes_read == 0)
                    {
                        client_active = false;
                    }
                }
                catch (const Poco::TimeoutException &)
                {
                    LOG_WARNING(log, "Timeout while reading from client, continuing...");
                }
                catch (const Poco::Exception & e)
                {
                    LOG_ERROR(log, "Error reading from client: {}", e.displayText());
                    client_active = false;
                }
            }

            if (std::find(read_ready.begin(), read_ready.end(), *mid_socket) != read_ready.end())
            {
                try
                {
                    int bytes_read = mid_socket->receiveBytes(buffer, buffer_size);
                    if (bytes_read > 0)
                    {
                        socket().sendBytes(buffer, bytes_read);
                    }
                    else if (bytes_read == 0)
                    {
                        server_active = false;
                    }
                }
                catch (const Poco::TimeoutException &)
                {
                    LOG_WARNING(log, "Timeout while reading from server, continuing...");
                }
                catch (const Poco::Exception & e)
                {
                    if (e.displayText().find("Connection reset by peer") != std::string::npos)
                    {
                        LOG_ERROR(log, "Error reading from server: {}", e.displayText());
                        server_active = false;
                    }
                    else
                    {
                        LOG_WARNING(log, "Non-fatal error from server: {}", e.displayText());
                        Poco::Thread::sleep(100); // TOOD: move to config
                    }
                }
            }

            if (read_ready.empty())
            {
                Poco::Thread::sleep(10); // TOOD: move to config
            }
        }
    }
    catch (...)
    {
        out->finalize();
        mid_out->finalize();
        throw;
    }

    if (client_active)
    {
        try
        {
            socket().shutdown();
        }
        catch (...)
        {
            LOG_WARNING(log, "Error shutting down client connection");
        }
    }

    if (server_active)
    {
        try
        {
            mid_socket->shutdown();
        }
        catch (...)
        {
            LOG_WARNING(log, "Error shutting down server connection");
        }
    }

    try
    {
        out->finalize();
    }
    catch (...)
    {
        LOG_ERROR(log, "Error finalizing client output buffer");
    }
    try
    {
        mid_out->finalize();
    }
    catch (...)
    {
        LOG_ERROR(log, "Error finalizing server output buffer");
    }

    LOG_DEBUG(log, "Proxy redirection completed");
}

} // namespace Proxy
