#if !(defined(__FreeBSD__) || defined(__APPLE__) || defined(_MSC_VER))

#include <boost/noncopyable.hpp>
#include <Common/Exception.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <IO/AIO.h>


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
    return syscall(__NR_io_setup, nr, ctxp);
}

int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

int io_submit(aio_context_t ctx, long nr, struct iocb * iocbpp[])
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

int io_getevents(aio_context_t ctx, long min_nr, long max_nr, io_event * events, struct timespec * timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


AIOContext::AIOContext(unsigned int nr_events)
{
    ctx = 0;
    if (io_setup(nr_events, &ctx) < 0)
        DB::throwFromErrno("io_setup failed", DB::ErrorCodes::CANNOT_IOSETUP);
}

AIOContext::~AIOContext()
{
    io_destroy(ctx);
}

#endif
