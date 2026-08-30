#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Poco/Net/StreamSocket.h>
#include <Common/EventFD.h>
#include <Common/Logger.h>
#include <atomic>
#include <future>

namespace DB
{

/// Represents a connection that may not be established yet.
/// Provides a file descriptor that can be polled (epoll on Linux, kqueue on macOS) to wait
/// asynchronously for the connection, backed by `EventFD` (a native eventfd on Linux, a
/// self-pipe on macOS).
class FutureConnection
{
public:
    FutureConnection();

    /// Get the notification file descriptor to register with the poller.
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
    /// Wake the poller via the notification fd after the promise is completed.
    void notifyWaiter() const;

    std::promise<Poco::Net::Socket> promise;
    std::shared_future<Poco::Net::Socket> future;
    /// Guards the single allowed promise completion: setSocket and cancel race (the peer connecting
    /// vs. query teardown), and the loser must be a no-op rather than throw "promise already set".
    std::atomic<bool> satisfied{false};
    /// Pollable wakeup fd: `event_fd.fd` is registered with the poller, and completion writes to it.
    /// One kernel fd on Linux (eventfd), a self-pipe pair on macOS.
    EventFD event_fd;
    LoggerPtr log = getLogger("FutureConnection");
};

using FutureConnectionPtr = std::shared_ptr<FutureConnection>;

}

#endif
