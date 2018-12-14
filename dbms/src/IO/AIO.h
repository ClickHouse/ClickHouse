#pragma once

#if defined(__linux__)

#include <boost/noncopyable.hpp>

/// https://stackoverflow.com/questions/20759750/resolving-redefinition-of-timespec-in-time-h
#define timespec linux_timespec
#define timeval linux_timeval
#define itimerspec linux_itimerspec
#define sigset_t linux_sigset_t

#include <linux/aio_abi.h>

#undef timespec
#undef timeval
#undef itimerspec
#undef sigset_t


/** Small wrappers for asynchronous I/O.
  */

int io_setup(unsigned nr, aio_context_t * ctxp);

int io_destroy(aio_context_t ctx);

/// last argument is an array of pointers technically speaking
int io_submit(aio_context_t ctx, long nr, struct iocb * iocbpp[]);

int io_getevents(aio_context_t ctx, long min_nr, long max_nr, io_event * events, struct timespec * timeout);


struct AIOContext : private boost::noncopyable
{
    aio_context_t ctx;

    AIOContext(unsigned int nr_events = 128);
    ~AIOContext();
};

#endif
