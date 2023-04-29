#include <Client/RemoteFSConnection.h>


namespace DB
{

RemoteFSConnection::RemoteFSConnection(const String & host_, UInt16 port_,
    const String & disk_name_)
    : host(host_), port(port_), disk_name(disk_name_)
    , log_wrapper(*this)
{
    /// Don't connect immediately, only on first need.

    setDescription();
}

RemoteFSConnection::~RemoteFSConnection() = default;

void RemoteFSConnection::connect(const ConnectionTimeouts & timeouts)
{
    try
    {
        LOG_TRACE(log_wrapper.get(), "Connecting. Disk: {}", disk_name);

        auto addresses = DNSResolver::instance().resolveAddressList(host, port);
        const auto & connection_timeout = static_cast<bool>(secure) ? timeouts.secure_connection_timeout : timeouts.connection_timeout;

        for (auto it = addresses.begin(); it != addresses.end();)
        {
            if (connected)
                disconnect();

            socket = std::make_unique<Poco::Net::StreamSocket>();

            try
            {
                socket->connect(*it, connection_timeout);
                current_resolved_address = *it;
                break;
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

        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);
        int tcp_keep_alive_timeout_in_sec = timeouts.tcp_keep_alive_timeout.totalSeconds();
        if (tcp_keep_alive_timeout_in_sec)
        {
            socket->setKeepAlive(true);
            socket->setOption(IPPROTO_TCP,
#if defined(TCP_KEEPALIVE)
                TCP_KEEPALIVE
#else
                TCP_KEEPIDLE  // __APPLE__
#endif
                , tcp_keep_alive_timeout_in_sec);
        }

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();

        LOG_TRACE(log_wrapper.get(), "Connected to remote disk {}", disk_name);
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Remove this possible stale entry from cache
        DNSResolver::instance().removeHostFromCache(host);

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(ErrorCodes::NETWORK_ERROR, "{} ({})", e.displayText(), getDescription());
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Remove this possible stale entry from cache
        DNSResolver::instance().removeHostFromCache(host);

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        /// This exception can only be thrown from socket->connect(), so add information about connection timeout.
        const auto & connection_timeout = static_cast<bool>(secure) ? timeouts.secure_connection_timeout : timeouts.connection_timeout;
        throw NetException(
            ErrorCodes::SOCKET_TIMEOUT,
            "{} ({}, connection timeout {} ms)",
            e.displayText(),
            getDescription(),
            connection_timeout.totalMilliseconds());
    }
}

void RemoteFSConnection::disconnect()
{
    in = nullptr;
    out = nullptr; // can write to socket
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

const String & RemoteFSConnection::getDescription() const
{
    return description;
}

const String & RemoteFSConnection::getHost() const
{
    return host;
}

UInt16 RemoteFSConnection::getPort() const
{
    return port;
}

std::optional<Poco::Net::SocketAddress> RemoteFSConnection::getResolvedAddress() const
{
    return current_resolved_address;
}

void RemoteFSConnection::setDescription()
{
    auto resolved_address = getResolvedAddress();
    description = host + ":" + toString(port);

    if (resolved_address)
    {
        auto ip_address = resolved_address->host().toString();
        if (host != ip_address)
            description += ", " + ip_address;
    }
}

void RemoteFSConnection::sendHello()
{
    writeVarUInt(RemoteFSProtocol::Hello, *out);
    writeStringBinary(disk_name, *out);
    out->next();
}

void RemoteFSConnection::receiveHello()
{
    /// Receive hello packet.
    UInt64 packet_type = 0;

    /// Prevent read after eof in readVarUInt in case of reset connection
    /// (Poco should throw such exception while reading from socket but
    /// sometimes it doesn't for unknown reason)
    if (in->eof())
        throw Poco::Net::NetException("Connection reset by peer");

    readVarUInt(packet_type, *in);
    if (packet_type == RemoteFSProtocol::Hello)
    {
        return
    }
    else if (packet_type == RemoteFSProtocol::Exception)
        receiveException()->rethrow();
    else
    {
        /// Close connection, to not stay in unsynchronised state.
        disconnect();
        throwUnexpectedPacket(packet_type, "Hello or Exception");
    }
}

std::unique_ptr<Exception> RemoteFSConnection::receiveException() const
{
    return std::make_unique<Exception>(readException(*in, "Received from " + getDescription(), true /* remote */));
}

void RemoteFSConnection::throwUnexpectedPacket(UInt64 packet_type, const char * expected) const
{
    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
            "Unexpected packet from server {} (expected {}, got {})",
                       getDescription(), expected, String(Protocol::Server::toString(packet_type)));
}

}
