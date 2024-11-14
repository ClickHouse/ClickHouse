#include <Poco/Net/NetException.h>

#include <base/scope_guard.h>

#include <IO/ReadBufferFromPocoSocket.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/AsyncTaskExecutor.h>
#include <Common/checkSSLReturnCode.h>

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
    extern const int LOGICAL_ERROR;
}

ssize_t ReadBufferFromPocoSocketBase::socketReceiveBytesImpl(char * ptr, size_t size)
{
    ssize_t bytes_read = 0;
    Stopwatch watch;

    SCOPE_EXIT({
        /// NOTE: it is quite inaccurate on high loads since the thread could be replaced by another one
        ProfileEvents::increment(ProfileEvents::NetworkReceiveElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::NetworkReceiveBytes, bytes_read);
    });

    CurrentMetrics::Increment metric_increment(CurrentMetrics::NetworkReceive);

    /// Add more details to exceptions.
    try
    {
        /// If async_callback is specified, set socket to non-blocking mode
        /// and try to read data from it, if socket is not ready for reading,
        /// run async_callback and try again later.
        /// It is expected that file descriptor may be polled externally.
        /// Note that send timeout is not checked here. External code should check it while polling.
        if (async_callback)
        {
            socket.setBlocking(false);
            SCOPE_EXIT(socket.setBlocking(true));
            bool secure = socket.secure();
            bytes_read = socket.impl()->receiveBytes(ptr, static_cast<int>(size));

            /// Check EAGAIN and ERR_SSL_WANT_READ/ERR_SSL_WANT_WRITE for secure socket (reading from secure socket can write too).
            while (bytes_read < 0 && (errno == EAGAIN || (secure && (checkSSLWantRead(bytes_read) || checkSSLWantWrite(bytes_read)))))
            {
                /// In case of ERR_SSL_WANT_WRITE we should wait for socket to be ready for writing, otherwise - for reading.
                if (secure && checkSSLWantWrite(bytes_read))
                    async_callback(socket.impl()->sockfd(), socket.getSendTimeout(), AsyncEventTimeoutType::SEND, socket_description, AsyncTaskExecutor::Event::WRITE | AsyncTaskExecutor::Event::ERROR);
                else
                    async_callback(socket.impl()->sockfd(), socket.getReceiveTimeout(), AsyncEventTimeoutType::RECEIVE, socket_description, AsyncTaskExecutor::Event::READ | AsyncTaskExecutor::Event::ERROR);

                /// Try to read again.
                bytes_read = socket.impl()->receiveBytes(ptr, static_cast<int>(size));
            }
        }
        else
        {
            bytes_read = socket.impl()->receiveBytes(ptr, static_cast<int>(size));
        }
    }
    catch (const Poco::Net::NetException & e)
    {
        throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while reading from socket (peer: {}, local: {})", e.displayText(), peer_address.toString(), socket.address().toString());
    }
    catch (const Poco::TimeoutException &)
    {
        throw NetException(ErrorCodes::SOCKET_TIMEOUT, "Timeout exceeded while reading from socket (peer: {}, local: {}, {} ms)",
            peer_address.toString(), socket.address().toString(),
            socket.impl()->getReceiveTimeout().totalMilliseconds());
    }
    catch (const Poco::IOException & e)
    {
        throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while reading from socket (peer: {}, local: {})", e.displayText(), peer_address.toString(), socket.address().toString());
    }

    if (bytes_read < 0)
        throw NetException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot read from socket (peer: {}, local: {})", peer_address.toString(), socket.address().toString());

    return bytes_read;
}

bool ReadBufferFromPocoSocketBase::nextImpl()
{
    if (internal_buffer.size() > INT_MAX)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer overflow");

    ssize_t bytes_read = socketReceiveBytesImpl(internal_buffer.begin(), internal_buffer.size());

    if (read_event != ProfileEvents::end())
        ProfileEvents::increment(read_event, bytes_read);

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;

    return true;
}

ReadBufferFromPocoSocketBase::ReadBufferFromPocoSocketBase(Poco::Net::Socket & socket_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size)
    , socket(socket_)
    , peer_address(socket.peerAddress())
    , read_event(ProfileEvents::end())
    , socket_description("socket (" + peer_address.toString() + ")")
{
}

ReadBufferFromPocoSocketBase::ReadBufferFromPocoSocketBase(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size)
    : ReadBufferFromPocoSocketBase(socket_, buf_size)
{
    read_event = read_event_;
}

bool ReadBufferFromPocoSocketBase::poll(size_t timeout_microseconds) const
{
    /// For secure socket it is important to check if any remaining data available in underlying decryption buffer -
    /// read always retrieves the whole encrypted frame from the wire and puts it into underlying buffer while returning only requested size -
    /// further poll() can block though there is still data to read in the underlying decryption buffer.
    if (available() || socket.impl()->available())
        return true;

    Stopwatch watch;
    bool res = socket.impl()->poll(timeout_microseconds, Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR);
    ProfileEvents::increment(ProfileEvents::NetworkReceiveElapsedMicroseconds, watch.elapsedMicroseconds());
    return res;
}

void ReadBufferFromPocoSocketBase::setReceiveTimeout(size_t receive_timeout_microseconds)
{
    socket.setReceiveTimeout(Poco::Timespan(receive_timeout_microseconds, 0));
}

}
