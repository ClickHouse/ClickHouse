#include <IO/SilkFiberStreamSocketImpl.h>

#if defined(OS_LINUX)

#include <Common/Exception.h>

#include <base/scope_guard.h>

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
    init(address.af());
    setBlocking(false);
    SCOPE_EXIT({ setBlocking(true); });

    int r = ::connect(sockfd(), address.addr(), address.length());
    if (r < 0)
    {
        r = errno;
        if (r != EINPROGRESS)
            error(r, address.toString());

        silk::FiberScheduler::IoFuture poll_future;
        silk::FiberScheduler::poll(sockfd(), POLLOUT, nullptr, &poll_future);

        Poco::Timestamp::TimeDiff timeout_us = timeout.totalMicroseconds();
        if (timeout_us >= 0)
        {
            r = silk::FiberFuture::waitWithTimeout(&poll_future, static_cast<uint64_t>(timeout_us) * 1000);
            if (r == ETIMEDOUT)
            {
                poll_future.cancel();
                (void)poll_future.wait();
            }
        }
        else
        {
            r = poll_future.wait();
        }

        if (r)
            error(r, address.toString());
    }

    r = socketError();
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
    int r;
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
    return (triggered & static_cast<uint64_t>(events)) != 0;
}

int FiberStreamSocketImpl::sendBytes(const void * buffer, int length, int flags)
{
    if (flags != 0)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Silk::FiberStreamSocketImpl::sendBytes: non-zero flags ({}) not supported",
            flags);

    int total = 0;
    const char * ptr = static_cast<const char *>(buffer);
    Poco::Timespan timeout = getSendTimeout();
    while (total < length)
    {
        uint64_t bytes_written = 0;
        silk::FiberScheduler::IoFuture future;
        iovec iov{const_cast<char *>(ptr), static_cast<size_t>(length - total)};
        silk::FiberScheduler::write(sockfd(), &iov, 1, 0, &bytes_written, &future);

        int r;
        if (timeout.totalMicroseconds() > 0)
        {
            r = silk::FiberFuture::waitWithTimeout(
                &future,
                static_cast<uint64_t>(timeout.totalMicroseconds()) * 1000);
            if (r == ETIMEDOUT)
            {
                future.cancel();
                (void)future.wait();
                throw Poco::TimeoutException("Send timed out", peerAddress().toString());
            }
        }
        else
        {
            r = future.wait();
        }

        if (r)
            error(r, "send");

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

    uint64_t bytes_read = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{buffer, static_cast<size_t>(length)};
    silk::FiberScheduler::read(sockfd(), &iov, 1, 0, &bytes_read, &future);

    Poco::Timespan timeout = getReceiveTimeout();
    int r;
    if (timeout.totalMicroseconds() > 0)
    {
        r = silk::FiberFuture::waitWithTimeout(
            &future,
            static_cast<uint64_t>(timeout.totalMicroseconds()) * 1000);
        if (r == ETIMEDOUT)
        {
            future.cancel();
            (void)future.wait();
            throw Poco::TimeoutException("Receive timed out", peerAddress().toString());
        }
    }
    else
    {
        r = future.wait();
    }

    if (r)
        error(r, "recv");
    return static_cast<int>(bytes_read);
}

}

#endif
