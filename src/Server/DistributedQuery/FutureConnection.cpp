#include <Server/DistributedQuery/FutureConnection.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FutureConnection::FutureConnection()
    : future(promise.get_future())
{
    LOG_TRACE(log, "Created FutureConnection");
}

int FutureConnection::getEventFd() const
{
    return event_fd.fd;
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
    event_fd.write();
}

}

#endif
