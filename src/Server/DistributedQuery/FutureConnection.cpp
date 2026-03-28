#ifdef OS_LINUX

#include <Server/DistributedQuery/FutureConnection.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
}

FutureConnection::FutureConnection()
    : future(promise.get_future())
    , event_fd(createEventFd())
{
    LOG_TRACE(log, "Created FutureConnection");
}

FutureConnection::~FutureConnection()
{
    [[maybe_unused]] int err = close(event_fd);
    chassert(!err || errno == EINTR);
}

int FutureConnection::createEventFd()
{
    auto fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd == -1)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Failed to create eventfd, error {}", errno);
    return fd;
}

int FutureConnection::getEventFd() const
{
    return event_fd;
}

bool FutureConnection::isReady() const
{
    return future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

Poco::Net::Socket FutureConnection::getSocket()
{
    if (!isReady())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FutureConnection does not have a ready future, check is Ready() before calling getSocket()");

    // since it is a shared_future, multiple calls to get() are allowed and will return the same socket once it is set.
    return future.get();
}

void FutureConnection::setSocket(Poco::Net::Socket socket)
{
    LOG_TRACE(log, "Setting socket for FutureConnection, eventfd: {}", event_fd);

    /// Set the promise value, promise can be set only once.
    promise.set_value(std::move(socket));

    uint64_t value = 1;
    ssize_t written = write(event_fd, &value, sizeof(value));
    chassert(written == sizeof(value));
}

}

#endif
