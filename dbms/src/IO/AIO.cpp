#if defined(__linux__)

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

#elif defined(__FreeBSD__)

#include <boost/noncopyable.hpp>
#include <Common/Exception.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <aio.h>

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
    long i;
    int r;
    struct sigevent *se;
    struct aiocb *iocb;

    for (i = 0; i < nr; i ++) {
	iocb = &iocbpp[i]->aio;

	se = &iocb->aio_sigevent;
	se->sigev_notify_kqueue = ctx;
	se->sigev_notify_kevent_flags = 0;
	se->sigev_notify = SIGEV_KEVENT;
	se->sigev_value.sival_ptr = iocbpp[i];

	switch(iocb->aio_lio_opcode) {
	    case LIO_READ:
		r = aio_read(iocb);
		break;
	    case LIO_WRITE:
		r = aio_write(iocb);
		break;
	    default: break;
	}
	if (r < 0) {
	    return r;
	}
    }

    return i;
}

int io_getevents(int ctx, long min_nr, long max_nr, struct kevent * events, struct timespec * timeout)
{
    min_nr = 0;
    return kevent(ctx, NULL, 0, events, max_nr, timeout);
}


AIOContext::AIOContext(unsigned int nr_events)
{
    nr_events = 0;
    ctx = io_setup();
    if (ctx < 0)
        DB::throwFromErrno("io_setup failed", DB::ErrorCodes::CANNOT_IOSETUP);
}

AIOContext::~AIOContext()
{
    io_destroy(ctx);
}

#endif
