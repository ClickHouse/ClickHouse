#include "TCPHandler.h"

#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Core/Defines.h>
#include <Core/Protocol.h>
#include <IO/LimitReadBuffer.h>
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
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int BAD_ARGUMENTS;
extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
extern const int UNEXPECTED_PACKET_FROM_CLIENT;
extern const int UNKNOWN_SOCKET_ADDRESS_FAMILY;
}

namespace Proxy
{

TCPHandler::TCPHandler(
    IProxyServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool parse_proxy_protocol_, RouterPtr router_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , parse_proxy_protocol(parse_proxy_protocol_)
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

    /// Support for PROXY protocol
    if (parse_proxy_protocol && !receiveProxyHeader())
        return;

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    out = std::make_shared<DB::AutoCanceledWriteBuffer<DB::WriteBufferFromPocoSocketChunked>>(socket());

    try
    {
        receiveHello();
        if (action->getType() == RuleActionType::Reject)
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
        doRedirection();
        action->disconnect();
    }
    catch (const DB::Exception & e)
    {
        if (e.code() == DB::ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        {
            LOG_DEBUG(log, "Client has connected to wrong port.");
            return;
        }

        if (e.code() == DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_INFO(log, "Client has gone away.");
            return;
        }

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

// TODO: move to lib
std::string formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(const Poco::Util::AbstractConfiguration & config)
{
    std::string result = fmt::format(
        "HTTP/1.0 400 Bad Request\r\n\r\n"
        "Port {} is for clickhouse-client program\r\n",
        config.getString("tcp_port"));

    // TODO: add hint using http port when supported

    return result;
}

}

void TCPHandler::generateProxyHeader()
{
    if (parse_proxy_protocol)
    {
        return;
    }

    proxy_header.clear();
    proxy_header.reserve(107);

    proxy_header += "PROXY TCP";

    const auto client_address = socket().peerAddress();
    const auto client_host = client_address.host();

    if (!target_socket) [[unlikely]]
    {
        throw std::runtime_error("target_socket is nullptr");
    }

    const auto target_address = target_socket->address();
    const auto target_host = target_address.host();

    bool is_ipv6 = client_address.family() == Poco::Net::SocketAddress::IPv6 || target_address.family() == Poco::Net::SocketAddress::IPv6;
    proxy_header += is_ipv6 ? "6 " : "4 ";

    switch (client_address.family())
    {
        case Poco::Net::SocketAddress::IPv4:
            proxy_header += (is_ipv6 ? "::ffff:" : "") + client_host.toString();
            break;
        case Poco::Net::SocketAddress::IPv6: {
            proxy_header += client_host.toString();
            break;
        }
        default: {
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_SOCKET_ADDRESS_FAMILY, "Unknown client socket address family");
        }
    }
    proxy_header += " ";

    switch (target_address.family())
    {
        case Poco::Net::SocketAddress::IPv4:
            proxy_header += (is_ipv6 ? "::ffff:" : "") + target_host.toString();
            break;
        case Poco::Net::SocketAddress::IPv6: {
            proxy_header += target_host.toString();
            break;
        }
        default: {
            throw DB::Exception(DB::ErrorCodes::UNKNOWN_SOCKET_ADDRESS_FAMILY, "Unknown target socket address family");
        }
    }
    proxy_header += " ";

    proxy_header += std::to_string(client_address.port()) + " ";
    proxy_header += std::to_string(target_address.port()) + "\r\n";
}

bool TCPHandler::receiveProxyHeader()
{
    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return false;
    }

    String forwarded_address;

    /// Only PROXYv1 is supported.
    /// Validation of protocol is not fully performed.

    DB::LimitReadBuffer limit_in(*in, {.read_no_more = 107, .expect_eof = true}); /// Maximum length from the specs.

    std::string aux;
    proxy_header.clear();
    proxy_header.reserve(107); // TODO: move to some constants header?

    assertString("PROXY ", limit_in);
    proxy_header += "PROXY ";

    if (limit_in.eof())
    {
        LOG_WARNING(log, "Incomplete PROXY header is received.");
        return false;
    }

    /// TCP4 / TCP6 / UNKNOWN
    if ('T' == *limit_in.position())
    {
        assertString("TCP", limit_in);
        proxy_header += "TCP";

        if (limit_in.eof())
        {
            LOG_WARNING(log, "Incomplete PROXY header is received.");
            return false;
        }

        if ('4' != *limit_in.position() && '6' != *limit_in.position())
        {
            LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
            return false;
        }

        bool is_tcp6 = ('6' == *limit_in.position());
        proxy_header += *limit_in.position();

        ++limit_in.position();
        assertChar(' ', limit_in);
        proxy_header += ' ';

        /// Read the first field and ignore other.
        readStringUntilWhitespace(forwarded_address, limit_in);
        proxy_header += forwarded_address;

        if (is_tcp6)
            forwarded_address = "[" + forwarded_address + "]";

        /// Skip second field (destination address)
        assertChar(' ', limit_in);
        readStringUntilWhitespace(aux, limit_in);
        assertChar(' ', limit_in);
        proxy_header += ' ' + aux + ' ';

        /// Read source port
        String port;
        readStringUntilWhitespace(port, limit_in);
        proxy_header += port;

        forwarded_address += ":" + port;

        /// Skip until \r\n
        while (!limit_in.eof() && *limit_in.position() != '\r')
        {
            proxy_header += *limit_in.position();
            ++limit_in.position();
        }
        assertString("\r\n", limit_in);
        proxy_header += "\r\n";
    }
    else if (checkString("UNKNOWN", limit_in))
    {
        /// This is just a health check, there is no subsequent data in this connection.

        while (!limit_in.eof() && *limit_in.position() != '\r')
            ++limit_in.position();
        assertString("\r\n", limit_in);
        return false;
    }
    else
    {
        LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
        return false;
    }

    LOG_TRACE(log, "Forwarded client address from PROXY header: {}", forwarded_address);
    forwarded_for = std::move(forwarded_address);
    return true;
}

void TCPHandler::sendProxyHeader()
{
    target_out->write(proxy_header.data(), proxy_header.size());
    target_out->next();
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

    action = router->route(user, hostname, default_database);

    switch (action->getType())
    {
        case RuleActionType::Reject: {
            LOG_DEBUG(log, "Rejecting connection via router rules");

            return;
        }
        case RuleActionType::Route: {
            client_connection_data = ClientConnectionData{
                .user = std::move(user),
                .password = std::move(password),
                .default_database = std::move(default_database),
                .hostname = std::move(hostname),
                .client_name = std::move(client_name),
                .client_version_major = client_version_major,
                .client_version_minor = client_version_minor,
                .client_version_patch = client_version_patch,
                .client_tcp_protocol_version = client_tcp_protocol_version};

            const auto & target = action->getTarget().value();

            if (target.tcp_port == 0) // TODO: check ports when parsing config
            {
                LOG_ERROR(log, "Cannot redirect TCP connection to server '{}', there is no 'tcp_port' provided", target.key);
                return;
            }

            LOG_DEBUG(log, "Redirecting connection to {}:{}", target.host, target.tcp_port);

            connect();
            generateProxyHeader();
            sendProxyHeader();
            redirectHello();
        }
    }
}

void TCPHandler::redirectHello()
{
    auto has_control_character = [](const std::string & s)
    {
        for (auto c : s)
            if (isControlASCII(c))
                return true;
        return false;
    };

    if (has_control_character(client_connection_data.default_database) || has_control_character(client_connection_data.user)
        || has_control_character(client_connection_data.password))
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Parameters 'default_database', 'user' and 'password' must not contain ASCII control characters");

    writeVarUInt(DB::Protocol::Client::Hello, *target_out);
    writeStringBinary(client_connection_data.client_name, *target_out);
    writeVarUInt(client_connection_data.client_version_major, *target_out);
    writeVarUInt(client_connection_data.client_version_minor, *target_out);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    writeVarUInt(client_connection_data.client_tcp_protocol_version, *target_out);
    writeStringBinary(client_connection_data.default_database, *target_out);

    writeStringBinary(client_connection_data.user, *target_out);
    writeStringBinary(client_connection_data.password, *target_out);

    target_out->next();
}

void TCPHandler::connect()
{
    const auto & target = action->getTarget().value();

    auto addresses = DB::DNSResolver::instance().resolveAddressList(target.host, target.tcp_port);
    const auto connection_timeout = DB::DBMS_DEFAULT_CONNECT_TIMEOUT_SEC; // TODO: move to config

    for (auto it = addresses.begin(); it != addresses.end();)
    {
        target_socket = std::make_unique<Poco::Net::StreamSocket>();

        try
        {
            target_socket->connect(*it, connection_timeout);
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

    target_socket->setReceiveTimeout(DB::DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC); // TODO: move to config
    target_socket->setSendTimeout(DB::DBMS_DEFAULT_SEND_TIMEOUT_SEC); // TODO: move to config
    target_socket->setNoDelay(true);

    // TODO: keepalive ?

    target_out = std::make_shared<DB::AutoCanceledWriteBuffer<DB::WriteBufferFromPocoSocketChunked>>(*target_socket);
}

void TCPHandler::doRedirection()
{
    const auto & assigned_server = action->getTarget().value();

    constexpr size_t buffer_size = 32 * 1024;
    char buffer[buffer_size];

    Poco::Net::Socket::SocketList read_list;
    read_list.push_back(socket());
    read_list.push_back(*target_socket);

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
                        target_socket->sendBytes(buffer, bytes_read);
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

            if (std::find(read_ready.begin(), read_ready.end(), *target_socket) != read_ready.end())
            {
                try
                {
                    int bytes_read = target_socket->receiveBytes(buffer, buffer_size);
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
                        Poco::Thread::sleep(100); // TODO: move to config
                    }
                }
            }

            if (read_ready.empty())
            {
                Poco::Thread::sleep(10); // TODO: move to config
            }
        }
    }
    catch (...)
    {
        out->finalize();
        target_out->finalize();
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
            target_socket->shutdown();
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
        target_out->finalize();
    }
    catch (...)
    {
        LOG_ERROR(log, "Error finalizing server output buffer");
    }

    LOG_DEBUG(log, "Proxy redirection completed");
}

}
