#include <IO/AIO.h>

#if defined(OS_LINUX)

#    include <Common/Exception.h>

#    include <sys/syscall.h>
#    include <unistd.h>
#    include <utility>


/** Small wrappers for asynchronous I/O.
  */

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_IOSETUP;
    }
}


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

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_IOSETUP;
}
}


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

#endif
