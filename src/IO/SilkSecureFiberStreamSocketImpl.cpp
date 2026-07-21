#include <IO/SilkSecureFiberStreamSocketImpl.h>

#if USE_SILK && USE_SSL

#include <IO/SilkFiberStreamSocketImpl.h>

#include <base/MemorySanitizer.h>
#include <base/defines.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>

#include <openssl/bio.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <poll.h>
#include <sys/uio.h>


namespace Silk
{

namespace
{

uint64_t timeoutNs(const Poco::Timespan & timeout)
{
    return static_cast<uint64_t>(timeout.totalMicroseconds()) * 1000ULL;
}

int silkBioRead(BIO * bio, char * buf, int len)
{
    auto * socket_impl = static_cast<Poco::Net::SocketImpl *>(BIO_get_data(bio));
    const int fd = socket_impl->sockfd();
    const uint64_t timeout_ns = timeoutNs(socket_impl->getReceiveTimeout());

    uint64_t bytes_read = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{buf, static_cast<size_t>(len)};
    silk::FiberScheduler::read(fd, &iov, 1, 0, &bytes_read, &future);

    int r = timeout_ns > 0
        ? silk::FiberFuture::waitWithTimeout(&future, timeout_ns)
        : future.wait();

    if (r == ETIMEDOUT)
    {
        future.cancel();
        r = future.wait();
        if (r == ECANCELED)
            r = ETIMEDOUT;
    }

    BIO_clear_retry_flags(bio);

    if (r == 0)
    {
        /// TODO(mstetsyuk): should be done at Silk level.
        __msan_unpoison(buf, bytes_read);

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
    auto * socket_impl = static_cast<Poco::Net::SocketImpl *>(BIO_get_data(bio));
    const int fd = socket_impl->sockfd();
    const uint64_t timeout_ns = timeoutNs(socket_impl->getSendTimeout());

    uint64_t bytes_written = 0;
    silk::FiberScheduler::IoFuture future;
    iovec iov{const_cast<char *>(buf), static_cast<size_t>(len)};
    silk::FiberScheduler::write(fd, &iov, 1, 0, &bytes_written, &future);

    int r = timeout_ns > 0
        ? silk::FiberFuture::waitWithTimeout(&future, timeout_ns)
        : future.wait();

    if (r == ETIMEDOUT)
    {
        future.cancel();
        r = future.wait();
        if (r == ECANCELED)
            r = ETIMEDOUT;
    }

    BIO_clear_retry_flags(bio);

    if (r == 0)
        return static_cast<int>(bytes_written);

    errno = r;
    if (BIO_sock_non_fatal_error(r) || r == ETIMEDOUT)
        BIO_set_retry_write(bio);
    return -1;
}

long silkBioCtrl(BIO * bio, int cmd, [[maybe_unused]] long larg, void * parg) // NOLINT(google-runtime-int)
{
    switch (cmd)
    {
        case BIO_C_SET_FD:
            // The fd is not stored here.
            // BIO data holds the underlying SocketImpl,
            // and the fd is read from it.
            BIO_set_init(bio, 1);
            return 1;
        case BIO_C_GET_FD:
        {
            auto * socket_impl = static_cast<Poco::Net::SocketImpl *>(BIO_get_data(bio));
            const int fd = socket_impl->sockfd();
            if (parg)
                *static_cast<int *>(parg) = fd;
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

class SilkRecursiveMutex final : public Poco::Net::SecureSocketImpl::RecursiveMutex
{
public:
    void lock() override
    {
        auto * self = silk::FiberScheduler::getCurrentFiber();
        if (owner.load(std::memory_order_relaxed) == self)
        {
            chassert(count > 0);
            ++count;
            return;
        }
        mutex.lock();
        owner.store(self, std::memory_order_relaxed);
        chassert(count == 0);
        count = 1;
    }

    void unlock() override
    {
        chassert(owner.load(std::memory_order_relaxed) == silk::FiberScheduler::getCurrentFiber());
        if (--count == 0)
        {
            owner.store(nullptr, std::memory_order_relaxed);
            mutex.unlock();
        }
    }

private:
    silk::FiberMutex mutex;
    std::atomic<silk::Fiber *> owner{nullptr};
    std::size_t count = 0;
};

}

SecureFiberStreamSocketImpl::SecureFiberStreamSocketImpl(Poco::Net::Context::Ptr context)
    : Poco::Net::SecureStreamSocketImpl(new FiberStreamSocketImpl, context)
{
    setBioMethod(silkBioMethod());
    setMutex(std::make_unique<SilkRecursiveMutex>());
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

}

#endif
