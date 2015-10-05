#pragma once

#include <linux/aio_abi.h>
#include <unistd.h>
#include <sys/syscall.h>

#include <boost/noncopyable.hpp>

#include <DB/Common/Exception.h>


/** Небольшие обёртки для асинхронного ввода-вывода.
  */


inline int io_setup(unsigned nr, aio_context_t *ctxp)
{
	return syscall(__NR_io_setup, nr, ctxp);
}

inline int io_destroy(aio_context_t ctx)
{
	return syscall(__NR_io_destroy, ctx);
}

inline int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp)
{
	return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

inline int io_getevents(aio_context_t ctx, long min_nr, long max_nr, io_event *events, struct timespec *timeout)
{
	return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}


struct AIOContext : private boost::noncopyable
{
	aio_context_t ctx;

	AIOContext(unsigned int nr_events = 128)
	{
		ctx = 0;
		if (io_setup(nr_events, &ctx) < 0)
			DB::throwFromErrno("io_setup failed");
	}

	~AIOContext()
	{
		io_destroy(ctx);
	}
};
