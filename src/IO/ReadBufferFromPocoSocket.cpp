#include <Poco/Net/NetException.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/TimeoutSetter.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>


namespace ProfileEvents
{
    extern const Event NetworkReceiveElapsedMicroseconds;
    extern const Event NetworkReceiveBytes;
}

namespace CurrentMetrics
{
    extern const Metric NetworkReceive;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
}


bool ReadBufferFromPocoSocket::nextImpl()
{
    ssize_t bytes_read = 0;
    Stopwatch watch;

    /// Add more details to exceptions.
    try
    {
        CurrentMetrics::Increment metric_increment(CurrentMetrics::NetworkReceive);

        /// If async_callback is specified, and read will block, run async_callback and try again later.
        /// It is expected that file descriptor may be polled externally.
        /// Note that receive timeout is not checked here. External code should check it while polling.
        while (async_callback && !socket.poll(0, Poco::Net::Socket::SELECT_READ))
            async_callback(socket.impl()->sockfd(), socket.getReceiveTimeout(), socket_description);

        bytes_read = socket.impl()->receiveBytes(internal_buffer.begin(), internal_buffer.size());
    }
    catch (const Poco::Net::NetException & e)
    {
        throw NetException(e.displayText() + ", while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (const Poco::TimeoutException &)
    {
        throw NetException("Timeout exceeded while reading from socket (" + peer_address.toString() + ")", ErrorCodes::SOCKET_TIMEOUT);
    }
    catch (const Poco::IOException & e)
    {
        throw NetException(e.displayText() + ", while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
    }

    if (bytes_read < 0)
        throw NetException("Cannot read from socket (" + peer_address.toString() + ")", ErrorCodes::CANNOT_READ_FROM_SOCKET);

    /// NOTE: it is quite inaccurate on high loads since the thread could be replaced by another one
    ProfileEvents::increment(ProfileEvents::NetworkReceiveElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::NetworkReceiveBytes, bytes_read);

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;

    return true;
}

ReadBufferFromPocoSocket::ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size)
    , socket(socket_)
    , peer_address(socket.peerAddress())
    , socket_description("socket (" + peer_address.toString() + ")")
{
}

bool ReadBufferFromPocoSocket::poll(size_t timeout_microseconds) const
{
    if (available())
        return true;

    Stopwatch watch;
    bool res = socket.poll(timeout_microseconds, Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR);
    ProfileEvents::increment(ProfileEvents::NetworkReceiveElapsedMicroseconds, watch.elapsedMicroseconds());
    return res;
}

}
