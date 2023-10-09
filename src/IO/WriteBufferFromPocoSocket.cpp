#include <Poco/Net/NetException.h>

#include <base/scope_guard.h>

#include <IO/WriteBufferFromPocoSocket.h>

#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>


namespace ProfileEvents
{
    extern const Event NetworkSendElapsedMicroseconds;
    extern const Event NetworkSendBytes;
}

namespace CurrentMetrics
{
    extern const Metric NetworkSend;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_WRITE_TO_SOCKET;
    extern const int LOGICAL_ERROR;
}


void WriteBufferFromPocoSocket::nextImpl()
{
    if (!offset())
        return;

    Stopwatch watch;
    size_t bytes_written = 0;

    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::NetworkSendElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::NetworkSendBytes, bytes_written);
    });

    while (bytes_written < offset())
    {
        ssize_t res = 0;

        /// Add more details to exceptions.
        try
        {
            CurrentMetrics::Increment metric_increment(CurrentMetrics::NetworkSend);
            char * pos = working_buffer.begin() + bytes_written;
            size_t size = offset() - bytes_written;
            if (size > INT_MAX)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer overflow");
            res = socket.impl()->sendBytes(pos, static_cast<int>(size));
        }
        catch (const Poco::Net::NetException & e)
        {
            throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while writing to socket ({} -> {})", e.displayText(),
                               our_address.toString(), peer_address.toString());
        }
        catch (const Poco::TimeoutException &)
        {
            throw NetException(ErrorCodes::SOCKET_TIMEOUT, "Timeout exceeded while writing to socket ({}, {} ms)",
                peer_address.toString(),
                socket.impl()->getSendTimeout().totalMilliseconds());
        }
        catch (const Poco::IOException & e)
        {
            throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while writing to socket ({} -> {})", e.displayText(),
                               our_address.toString(), peer_address.toString());
        }

        if (res < 0)
            throw NetException(ErrorCodes::CANNOT_WRITE_TO_SOCKET, "Cannot write to socket ({} -> {})",
                               our_address.toString(), peer_address.toString());

        bytes_written += res;
    }
}

WriteBufferFromPocoSocket::WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), socket(socket_), peer_address(socket.peerAddress()), our_address(socket.address())
{
}

WriteBufferFromPocoSocket::~WriteBufferFromPocoSocket()
{
    finalize();
}

}
