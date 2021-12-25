#ifndef _AIO_H
#define _AIO_H

#ifdef __cplusplus
extern "C" {
#endif

#include <features.h>
#include <signal.h>
#include <time.h>

#define __NEED_ssize_t
#define __NEED_off_t

#include <bits/alltypes.h>

struct aiocb {
	int aio_fildes, aio_lio_opcode, aio_reqprio;
	volatile void *aio_buf;
	size_t aio_nbytes;
	struct sigevent aio_sigevent;
	void *__td;
	int __lock[2];
	volatile int __err;
	ssize_t __ret;
	off_t aio_offset;
	void *__next, *__prev;
	char __dummy4[32-2*sizeof(void *)];
};

#define AIO_CANCELED 0
#define AIO_NOTCANCELED 1
#define AIO_ALLDONE 2

#define LIO_READ 0
#define LIO_WRITE 1
#define LIO_NOP 2

#define LIO_WAIT 0
#define LIO_NOWAIT 1

int aio_read(struct aiocb *);
int aio_write(struct aiocb *);
int aio_error(const struct aiocb *);
ssize_t aio_return(struct aiocb *);
int aio_cancel(int, struct aiocb *);
int aio_suspend(const struct aiocb *const [], int, const struct timespec *);
int aio_fsync(int, struct aiocb *);

int lio_listio(int, struct aiocb *__restrict const *__restrict, int, struct sigevent *__restrict);

#if defined(_LARGEFILE64_SOURCE) || defined(_GNU_SOURCE)
#define aiocb64 aiocb
#define aio_read64 aio_read
#define aio_write64 aio_write
#define aio_error64 aio_error
#define aio_return64 aio_return
#define aio_cancel64 aio_cancel
#define aio_suspend64 aio_suspend
#define aio_fsync64 aio_fsync
#define lio_listio64 lio_listio
#define off64_t off_t
#endif

#if _REDIR_TIME64
__REDIR(aio_suspend, __aio_suspend_time64);
#endif

#ifdef __cplusplus
}
#endif

#endif
