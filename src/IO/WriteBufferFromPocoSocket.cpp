#include <Poco/Net/NetException.h>

#include <base/scope_guard.h>

#include <IO/WriteBufferFromPocoSocket.h>

#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/AsyncTaskExecutor.h>
#include <Common/checkSSLReturnCode.h>

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

ssize_t WriteBufferFromPocoSocket::socketSendBytesImpl(const char * ptr, size_t size)
{
    ssize_t res = 0;

    /// If async_callback is specified, set socket to non-blocking mode
    /// and try to write data to it, if socket is not ready for writing,
    /// run async_callback and try again later.
    /// It is expected that file descriptor may be polled externally.
    /// Note that send timeout is not checked here. External code should check it while polling.
    if (async_callback)
    {
        socket.setBlocking(false);
        /// Set socket to blocking mode at the end.
        SCOPE_EXIT(socket.setBlocking(true));
        bool secure = socket.secure();
        res = socket.impl()->sendBytes(ptr, static_cast<int>(size));

        /// Check EAGAIN and ERR_SSL_WANT_WRITE/ERR_SSL_WANT_READ for secure socket (writing to secure socket can read too).
        while (res < 0 && (errno == EAGAIN || (secure && (checkSSLWantRead(res) || checkSSLWantWrite(res)))))
        {
            /// In case of ERR_SSL_WANT_READ we should wait for socket to be ready for reading, otherwise - for writing.
            if (secure && checkSSLWantRead(res))
                async_callback(socket.impl()->sockfd(), socket.getReceiveTimeout(), AsyncEventTimeoutType::RECEIVE, socket_description, AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);
            else
                async_callback(socket.impl()->sockfd(), socket.getSendTimeout(), AsyncEventTimeoutType::SEND, socket_description, AsyncTaskExecutor::Event::WRITE | AsyncTaskExecutor::Event::ERROR);

            /// Try to write again.
            res = socket.impl()->sendBytes(ptr, static_cast<int>(size));
        }
    }
    else
    {
        res = socket.impl()->sendBytes(ptr, static_cast<int>(size));
    }

    return res;
}

void WriteBufferFromPocoSocket::socketSendBytes(const char * ptr, size_t size)
{
    if (!size)
        return;

    Stopwatch watch;
    size_t bytes_written = 0;

    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::NetworkSendElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::NetworkSendBytes, bytes_written);
        if (write_event != ProfileEvents::end())
            ProfileEvents::increment(write_event, bytes_written);
    });

    while (bytes_written < size)
    {
        ssize_t res = 0;

        /// Add more details to exceptions.
        try
        {
            CurrentMetrics::Increment metric_increment(CurrentMetrics::NetworkSend);
            if (size > INT_MAX)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer overflow");

            res = socketSendBytesImpl(ptr + bytes_written, size - bytes_written);
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

void WriteBufferFromPocoSocket::nextImpl()
{
    if (!offset())
        return;

    Stopwatch watch;
    size_t bytes_written = 0;

    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::NetworkSendElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::NetworkSendBytes, bytes_written);
        if (write_event != ProfileEvents::end())
            ProfileEvents::increment(write_event, bytes_written);
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

            res = socketSendBytesImpl(pos, size);
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
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , socket(socket_)
    , peer_address(socket.peerAddress())
    , our_address(socket.address())
    , write_event(ProfileEvents::end())
    , socket_description("socket (" + peer_address.toString() + ")")
{
}

WriteBufferFromPocoSocket::WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size)
    : WriteBufferFromPocoSocket(socket_, buf_size)
{
    write_event = write_event_;
}

WriteBufferFromPocoSocket::~WriteBufferFromPocoSocket()
{
    try
    {
        if (!canceled)
            finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
