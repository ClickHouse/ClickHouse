#include <IO/AIO.h>
#include <Common/ErrnoException.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_IOSETUP;
}

#if defined(OS_LINUX)

#    include <Common/Exception.h>

#    include <sys/syscall.h>
#    include <unistd.h>
#    include <utility>


/** Small wrappers for asynchronous I/O.
  */

int io_setup(unsigned nr, aio_context_t * ctxp)
{
    return static_cast<int>(syscall(__NR_io_setup, nr, ctxp));
}

int io_destroy(aio_context_t ctx)
{
    return static_cast<int>(syscall(__NR_io_destroy, ctx));
}

int io_submit(aio_context_t ctx, long nr, struct iocb * iocbpp[]) // NOLINT
{
    return static_cast<int>(syscall(__NR_io_submit, ctx, nr, iocbpp));
}

int io_getevents(aio_context_t ctx, long min_nr, long max_nr, io_event * events, struct timespec * timeout) // NOLINT
{
    return static_cast<int>(syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout));
}


AIOContext::AIOContext(unsigned int nr_events)
{
    ctx = 0;
    if (io_setup(nr_events, &ctx) < 0)
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_IOSETUP, "io_setup failed");
}

AIOContext::~AIOContext()
{
    if (ctx)
        io_destroy(ctx);
}

AIOContext::AIOContext(AIOContext && rhs) noexcept
{
    *this = std::move(rhs);
}

AIOContext & AIOContext::operator=(AIOContext && rhs) noexcept
{
    std::swap(ctx, rhs.ctx);
    return *this;
}

#elif defined(OS_FREEBSD)

#    include <Common/Exception.h>


/** Small wrappers for asynchronous I/O.
  */

int io_setup(void)
{
    return kqueue();
}

int io_destroy(int ctx)
{
    return close(ctx);
}

int io_submit(int ctx, long nr, struct iocb * iocbpp[])
{
    for (long i = 0; i < nr; ++i)
    {
        struct aiocb * iocb = &iocbpp[i]->aio;

        struct sigevent * se = &iocb->aio_sigevent;
        se->sigev_notify_kqueue = ctx;
        se->sigev_notify_kevent_flags = 0;
        se->sigev_notify = SIGEV_KEVENT;
        se->sigev_value.sival_ptr = iocbpp[i];

        switch (iocb->aio_lio_opcode)
        {
            case LIO_READ:
            {
                int r = aio_read(iocb);
                if (r < 0)
                    return r;
                break;
            }
            case LIO_WRITE:
            {
                int r = aio_write(iocb);
                if (r < 0)
                    return r;
                break;
            }
        }
    }

    return static_cast<int>(nr);
}

int io_getevents(int ctx, long, long max_nr, struct kevent * events, struct timespec * timeout)
{
    return kevent(ctx, nullptr, 0, events, static_cast<int>(max_nr), timeout);
}


AIOContext::AIOContext(unsigned int)
{
    ctx = io_setup();
    if (ctx < 0)
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_IOSETUP, "io_setup failed");
}

AIOContext::~AIOContext()
{
    io_destroy(ctx);
}

#elif defined(OS_DARWIN)

#    include <cerrno>
#    include <unistd.h>
#    include <Common/DequeWithMemoryTracking.h>


/** Synchronous emulation of the asynchronous I/O interface for macOS.
  * The I/O is performed immediately in io_submit; io_getevents just drains the completed results.
  */

struct DarwinAIOContext
{
    /// Results recorded by io_submit and drained (FIFO) by io_getevents.
    DB::DequeWithMemoryTracking<io_event> completed;
};

int io_setup(unsigned, aio_context_t * ctxp)
{
    *ctxp = new DarwinAIOContext;
    return 0;
}

int io_destroy(aio_context_t ctx)
{
    delete ctx;
    return 0;
}

int io_submit(aio_context_t ctx, long nr, struct iocb * iocbpp[])
{
    for (long i = 0; i < nr; ++i)
    {
        const struct aiocb * cb = &iocbpp[i]->aio;
        void * buffer = const_cast<void *>(cb->aio_buf);

        ssize_t res;
        if (cb->aio_lio_opcode == LIO_WRITE)
            res = ::pwrite(cb->aio_fildes, buffer, cb->aio_nbytes, cb->aio_offset);
        else
            res = ::pread(cb->aio_fildes, buffer, cb->aio_nbytes, cb->aio_offset);

        io_event event{};
        event.data = iocbpp[i]->aio_data;
        event.res = res;
        ctx->completed.push_back(event);
    }

    return static_cast<int>(nr);
}

int io_getevents(aio_context_t ctx, long, long max_nr, struct io_event * events, struct timespec *)
{
    long count = 0;
    while (count < max_nr && !ctx->completed.empty())
    {
        events[count] = ctx->completed.front();
        ctx->completed.pop_front();
        ++count;
    }

    return static_cast<int>(count);
}

AIOContext::AIOContext(unsigned int)
{
    ctx = new DarwinAIOContext;
}

AIOContext::~AIOContext()
{
    delete ctx;
}

AIOContext::AIOContext(AIOContext && rhs) noexcept
{
    ctx = rhs.ctx;
    rhs.ctx = nullptr;
}

AIOContext & AIOContext::operator=(AIOContext && rhs) noexcept
{
    std::swap(ctx, rhs.ctx);
    return *this;
}

#endif
