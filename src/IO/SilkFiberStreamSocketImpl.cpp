#include <IO/SilkFiberStreamSocketImpl.h>

#if USE_SILK

#include <Common/Exception.h>

#include <Poco/Exception.h>
#include <Poco/Net/StreamSocket.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <cerrno>

#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace Silk
{

FiberStreamSocketImpl::FiberStreamSocketImpl(int sockfd)
    : Poco::Net::StreamSocketImpl(sockfd)
{
}

void FiberStreamSocketImpl::connect(const Poco::Net::SocketAddress & address)
{
    connect(address, Poco::Timespan(-1));
}

void FiberStreamSocketImpl::connect(const Poco::Net::SocketAddress & address, const Poco::Timespan & timeout)
{
    if (sockfd() == POCO_INVALID_SOCKET)
        init(address.af());

    silk::FiberScheduler::IoFuture future;
    silk::FiberScheduler::connect(sockfd(), address.addr(), address.length(), &future);

    int r = 0;
    const Poco::Timestamp::TimeDiff timeout_us = timeout.totalMicroseconds();
    if (timeout_us >= 0)
    {
        r = silk::FiberFuture::waitWithTimeout(&future, static_cast<uint64_t>(timeout_us) * 1000);
        if (r == ETIMEDOUT)
        {
            future.cancel();
            (void)future.wait();
        }
    }
    else
    {
        r = future.wait();
    }

    if (r)
        error(r, address.toString());
}

bool FiberStreamSocketImpl::pollImpl(Poco::Timespan & timeout, int mode)
{
    uint32_t events = 0;
    if (mode & SELECT_READ)
        events |= POLLIN;
    if (mode & SELECT_WRITE)
        events |= POLLOUT;
    if (mode & SELECT_ERROR)
        events |= POLLERR;

    uint64_t triggered = 0;
    silk::FiberScheduler::IoFuture poll_future;
    silk::FiberScheduler::poll(sockfd(), events, &triggered, &poll_future);

    const Poco::Timestamp started;
    int r = 0;
    const Poco::Timestamp::TimeDiff timeout_us = timeout.totalMicroseconds();
    if (timeout_us >= 0)
    {
        r = silk::FiberFuture::waitWithTimeout(&poll_future, static_cast<uint64_t>(timeout_us) * 1000);
        if (r == ETIMEDOUT)
        {
            poll_future.cancel();
            (void)poll_future.wait();
            timeout = 0;
            return false;
        }
    }
    else
    {
        r = poll_future.wait();
    }

    const Poco::Timespan elapsed = Poco::Timestamp() - started;
    timeout = (elapsed < timeout) ? (timeout - elapsed) : Poco::Timespan(0);

    if (r)
        error(r, "poll");

    return triggered != 0;
}

int FiberStreamSocketImpl::sendBytes(const void * buffer, int length, int flags)
{
    if (flags != 0)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Silk::FiberStreamSocketImpl::sendBytes: non-zero flags ({}) not supported",
            flags);

    chassert(getBlocking());

    int total = 0;
    const char * ptr = static_cast<const char *>(buffer);
    Poco::Timespan remaining = getSendTimeout();
    const bool has_timeout = remaining.totalMicroseconds() > 0;

    while (total < length)
    {
        throttleSend(static_cast<size_t>(length - total), getBlocking());

        uint64_t bytes_written = 0;
        silk::FiberScheduler::IoFuture future;
        iovec iov{const_cast<char *>(ptr), static_cast<size_t>(length - total)};
        silk::FiberScheduler::write(sockfd(), &iov, 1, 0, &bytes_written, &future);

        int r = 0;
        if (has_timeout)
        {
            if (remaining.totalMicroseconds() <= 0)
                throw Poco::TimeoutException("Send timed out", peerAddress().toString());

            const Poco::Timestamp started;
            r = silk::FiberFuture::waitWithTimeout(
                &future,
                static_cast<uint64_t>(remaining.totalMicroseconds()) * 1000);
            if (r == ETIMEDOUT)
            {
                future.cancel();
                r = future.wait();
                if (r == ECANCELED)
                    throw Poco::TimeoutException("Send timed out", peerAddress().toString());
            }

            const Poco::Timespan elapsed = Poco::Timestamp() - started;
            remaining = (elapsed < remaining) ? (remaining - elapsed) : Poco::Timespan(0);
        }
        else
        {
            r = future.wait();
        }

        if (r)
            error(r, "send");

        useSendThrottlerBudget(static_cast<int>(bytes_written));

        total += static_cast<int>(bytes_written);
        ptr += bytes_written;
    }
    return total;
}

int FiberStreamSocketImpl::receiveBytes(void * buffer, int length, int flags)
{
    if (flags != 0)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Silk::FiberStreamSocketImpl::receiveBytes: non-zero flags ({}) not supported",
            flags);

    throttleRecv(static_cast<size_t>(length), getBlocking());

    uint64_t bytes_read = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{buffer, static_cast<size_t>(length)};
    silk::FiberScheduler::read(sockfd(), &iov, 1, 0, &bytes_read, &future);

    Poco::Timespan timeout = getReceiveTimeout();
    int r = 0;
    if (timeout.totalMicroseconds() > 0)
    {
        r = silk::FiberFuture::waitWithTimeout(
            &future,
            static_cast<uint64_t>(timeout.totalMicroseconds()) * 1000);
        if (r == ETIMEDOUT)
        {
            future.cancel();
            r = future.wait();
            if (r == ECANCELED)
                throw Poco::TimeoutException("Receive timed out", peerAddress().toString());
        }
    }
    else
    {
        r = future.wait();
    }

    if (r)
        error(r, "recv");

    useRecvThrottlerBudget(static_cast<int>(bytes_read));

    return static_cast<int>(bytes_read);
}

void FiberStreamSocketImpl::setBlocking(bool flag)
{
    if (!flag)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Non-blocking mode is not supported for Silk fibers aware sockets.");

    Poco::Net::StreamSocketImpl::setBlocking(flag);
}

}

#endif
