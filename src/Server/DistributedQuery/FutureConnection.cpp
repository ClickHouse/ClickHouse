#ifdef OS_LINUX

#include <Server/DistributedQuery/FutureConnection.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
}

FutureConnection::FutureConnection()
    : future(promise.get_future())
{
    LOG_TRACE(log, "Created FutureConnection (eventfd will be created lazily if needed)");
}

FutureConnection::~FutureConnection()
{
    if (event_fd != -1)
    {
        [[maybe_unused]] int err = close(event_fd);
        chassert(!err || errno == EINTR);
        event_fd = -1;
    }
}

void FutureConnection::ensureEventFd()
{
    std::lock_guard lock(event_fd_mutex);
    if (event_fd == -1)
    {
        event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (event_fd == -1)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to create eventfd, error {}", errno);

        LOG_TRACE(log, "Created eventfd: {}", event_fd);
    }
}

int FutureConnection::getEventFd()
{
    /// If socket is already ready, no need to create eventfd
    if (ready.load(std::memory_order_acquire))
    {
        LOG_TRACE(log, "Socket already ready, eventfd not needed");
        return -1;
    }

    ensureEventFd();
    return event_fd;
}

bool FutureConnection::isReady() const
{
    return ready.load(std::memory_order_acquire);
}

std::unique_ptr<Poco::Net::StreamSocket> FutureConnection::tryGetSocket()
{
    if (!isReady())
        return nullptr;

    /// Future is ready, get the socket
    try
    {
        auto socket = future.get();
        return std::make_unique<Poco::Net::StreamSocket>(socket);
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to get socket from future");
        throw;
    }
}

void FutureConnection::setSocket(Poco::Net::StreamSocket socket)
{
    LOG_TRACE(log, "Setting socket for FutureConnection");

    /// Set the promise value
    promise.set_value(std::move(socket));

    /// Mark as ready
    ready.store(true, std::memory_order_release);

    /// Signal the eventfd if it was created
    std::lock_guard lock(event_fd_mutex);
    if (event_fd != -1)
    {
        uint64_t value = 1;
        ssize_t written = write(event_fd, &value, sizeof(value));
        if (written != sizeof(value))
        {
            LOG_WARNING(log, "Failed to write to eventfd, error {}", errno);
        }
        else
        {
            LOG_TRACE(log, "Signaled eventfd: {}", event_fd);
        }
    }
    else
    {
        LOG_TRACE(log, "Socket set before eventfd was created (no waiting needed)");
    }
}

}

#endif
