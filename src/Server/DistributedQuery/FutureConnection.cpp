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
    /// First completion wins; a later setSocket/cancel is a no-op (the connection already paired
    /// or the query was torn down).
    if (satisfied.exchange(true))
        return;

    LOG_TRACE(log, "Setting socket for FutureConnection");
    promise.set_value(std::move(socket));
    notifyWaiter();
}

void FutureConnection::cancel(std::exception_ptr exception)
{
    if (satisfied.exchange(true))
        return;

    LOG_TRACE(log, "Cancelling FutureConnection");
    promise.set_exception(std::move(exception));
    notifyWaiter();
}

void FutureConnection::notifyWaiter() const
{
    uint64_t value = 1;
    ssize_t written = 0;
    /// Retry on EINTR so a signal does not leave the promise ready while the epoll waiter is never
    /// woken. Other write failures cannot happen for a non-full, valid eventfd.
    do
        written = write(event_fd, &value, sizeof(value));
    while (written < 0 && errno == EINTR);
    chassert(written == sizeof(value));
}

}

#endif
