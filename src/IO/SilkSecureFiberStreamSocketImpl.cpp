#include <IO/SilkSecureFiberStreamSocketImpl.h>

#if defined(OS_LINUX)

#include <IO/SilkFiberStreamSocketImpl.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <openssl/bio.h>

#include <cerrno>
#include <cstdint>

#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/uio.h>


namespace Silk
{

namespace
{

uint64_t readSocketTimeoutNs(int fd, int optname)
{
    timeval tv{};
    socklen_t opt_len = sizeof(tv);
    getsockopt(fd, SOL_SOCKET, optname, &tv, &opt_len);
    return (static_cast<uint64_t>(tv.tv_sec) * 1'000'000ULL + tv.tv_usec) * 1000ULL;
}

int silkBioRead(BIO * bio, char * buf, int len)
{
    const intptr_t fd_p = reinterpret_cast<intptr_t>(BIO_get_data(bio));
    const int fd = static_cast<int>(fd_p);
    const uint64_t timeout_ns = readSocketTimeoutNs(fd, SO_RCVTIMEO);

    uint64_t bytes_read = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{buf, static_cast<size_t>(len)};
    silk::FiberScheduler::read(fd, &iov, 1, 0, &bytes_read, &future);

    const int r = timeout_ns > 0
        ? silk::FiberFuture::waitWithTimeout(&future, timeout_ns)
        : future.wait();

    if (r == ETIMEDOUT)
    {
        future.cancel();
        (void)future.wait();
    }

    BIO_clear_retry_flags(bio);

    if (r == 0)
    {
        if (bytes_read == 0)
            BIO_set_flags(bio, BIO_FLAGS_IN_EOF);
        return static_cast<int>(bytes_read);
    }

    errno = r;
    if (BIO_sock_non_fatal_error(r) || r == ETIMEDOUT)
        BIO_set_retry_read(bio);
    return -1;
}

int silkBioWrite(BIO * bio, const char * buf, int len)
{
    const intptr_t fd_p = reinterpret_cast<intptr_t>(BIO_get_data(bio));
    const int fd = static_cast<int>(fd_p);
    const uint64_t timeout_ns = readSocketTimeoutNs(fd, SO_SNDTIMEO);

    uint64_t bytes_written = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{const_cast<char *>(buf), static_cast<size_t>(len)};
    silk::FiberScheduler::write(fd, &iov, 1, 0, &bytes_written, &future);

    const int r = timeout_ns > 0
        ? silk::FiberFuture::waitWithTimeout(&future, timeout_ns)
        : future.wait();

    if (r == ETIMEDOUT)
    {
        future.cancel();
        (void)future.wait();
    }

    BIO_clear_retry_flags(bio);

    if (r == 0)
        return static_cast<int>(bytes_written);

    errno = r;
    if (BIO_sock_non_fatal_error(r) || r == ETIMEDOUT)
        BIO_set_retry_write(bio);
    return -1;
}

long silkBioCtrl(BIO * bio, int cmd, [[maybe_unused]] long larg, void * parg)
{
    switch (cmd)
    {
        case BIO_C_SET_FD:
            BIO_set_data(bio, reinterpret_cast<void *>(static_cast<intptr_t>(*static_cast<int *>(parg))));
            BIO_set_init(bio, 1);
            return 1;
        case BIO_C_GET_FD:
        {
            const intptr_t fd = reinterpret_cast<intptr_t>(BIO_get_data(bio));
            if (parg)
                *static_cast<int *>(parg) = static_cast<int>(fd);
            return fd;
        }
        case BIO_CTRL_FLUSH:
            return 1;
        case BIO_CTRL_EOF:
            return BIO_test_flags(bio, BIO_FLAGS_IN_EOF);
        default:
            return 0;
    }
}

int silkBioCreate(BIO * bio)
{
    BIO_set_init(bio, 0);
    BIO_set_data(bio, nullptr);
    return 1;
}

int silkBioDestroy([[maybe_unused]] BIO * bio)
{
    return 1;
}

const BIO_METHOD * silkBioMethod()
{
    static const BIO_METHOD * method = []
    {
        BIO_METHOD * m = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "silk-fiber");
        BIO_meth_set_read(m, silkBioRead);
        BIO_meth_set_write(m, silkBioWrite);
        BIO_meth_set_ctrl(m, silkBioCtrl);
        BIO_meth_set_create(m, silkBioCreate);
        BIO_meth_set_destroy(m, silkBioDestroy);
        return m;
    }();
    return method;
}

}

SecureFiberStreamSocketImpl::SecureFiberStreamSocketImpl(Poco::Net::Context::Ptr context)
    : Poco::Net::SecureStreamSocketImpl(new FiberStreamSocketImpl, context)
{
    setBioMethod(silkBioMethod());
}

bool SecureFiberStreamSocketImpl::pollImpl(Poco::Timespan & timeout, int mode)
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

}

#endif
