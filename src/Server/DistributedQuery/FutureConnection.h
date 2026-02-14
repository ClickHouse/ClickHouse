#pragma once

#ifdef OS_LINUX

#include <Poco/Net/StreamSocket.h>
#include <Common/Logger.h>
#include <future>
#include <memory>

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
    int getEventFd();

    /// Check if the connection is ready (non-blocking)
    bool isReady() const;

    /// Try to get the socket if ready, otherwise return nullptr (non-blocking)
    std::unique_ptr<Poco::Net::StreamSocket> tryGetSocket();

    /// Set the socket value (called when connection is established)
    void setSocket(Poco::Net::StreamSocket socket);

    /// Check if this FutureConnection has been retrieved (getConnection was called)
    bool wasRetrieved() const { return retrieved.load(std::memory_order_acquire); }

    /// Mark as retrieved (called by getConnection)
    void markRetrieved() { retrieved.store(true, std::memory_order_release); }

private:
    void ensureEventFd();

    std::mutex event_fd_mutex;
    int event_fd = -1;
    std::promise<Poco::Net::StreamSocket> promise;
    std::shared_future<Poco::Net::StreamSocket> future;
    std::atomic<bool> ready{false};
    std::atomic<bool> retrieved{false};
    LoggerPtr log = getLogger("FutureConnection");
};

using FutureConnectionPtr = std::shared_ptr<FutureConnection>;

}

#endif
