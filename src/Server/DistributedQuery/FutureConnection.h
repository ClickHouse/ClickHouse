#pragma once

#ifdef OS_LINUX

#include <Poco/Net/StreamSocket.h>
#include <Common/Logger.h>
#include <future>

namespace DB
{

/// Represents a connection that may not be established yet.
/// Provides an eventfd that can be used with epoll to wait asynchronously for the connection.
class FutureConnection
{
public:
    FutureConnection();
    ~FutureConnection();

    /// Get the eventfd file descriptor for epoll (creates it lazily if needed)
    int getEventFd() const;

    /// Check if the connection is ready (non-blocking)
    bool isReady() const;

    /// Try to get the socket
    /// Should only be called once the connection is ready, otherwise it will throw an exception.
    /// Could be called multiple times after connection is ready and will return the same socket.
    Poco::Net::Socket getSocket();

    /// Set the socket value (called when connection is established)
    /// Should be called only once, subsequent calls will throw an exception.
    void setSocket(Poco::Net::Socket socket);

    /// Wake the waiter with an exception. Used to cancel a still-pending
    /// connection (e.g. when the owning query is being torn down).
    /// At-most-once like `setSocket`.
    void cancel(std::exception_ptr exception);

private:
    static int createEventFd();

    std::promise<Poco::Net::Socket> promise;
    std::shared_future<Poco::Net::Socket> future;
    int event_fd;
    LoggerPtr log = getLogger("FutureConnection");
};

using FutureConnectionPtr = std::shared_ptr<FutureConnection>;

}

#endif
