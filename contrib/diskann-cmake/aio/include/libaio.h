/* /usr/include/libaio.h
 *
 * Copyright 2000,2001,2002 Red Hat, Inc.
 *
 * Written by Benjamin LaHaise <bcrl@redhat.com>
 *
 * libaio Linux async I/O interface
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
#ifndef __LIBAIO_H
#define __LIBAIO_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <string.h>
#include <signal.h>

struct timespec;
struct sockaddr;
struct iovec;

typedef struct io_context *io_context_t;

typedef enum io_iocb_cmd {
	IO_CMD_PREAD = 0,
	IO_CMD_PWRITE = 1,

	IO_CMD_FSYNC = 2,
	IO_CMD_FDSYNC = 3,

	IO_CMD_POLL = 5,
	IO_CMD_NOOP = 6,
	IO_CMD_PREADV = 7,
	IO_CMD_PWRITEV = 8,
} io_iocb_cmd_t;

/* little endian, 32 bits */
#if defined(__i386__) || (defined(__arm__) && !defined(__ARMEB__)) || \
    (defined(__sh__) && defined(__LITTLE_ENDIAN__)) || \
    defined(__bfin__) || \
    (defined(__MIPSEL__) && !defined(__mips64)) || \
    defined(__cris__) || (defined(__riscv) && __riscv_xlen == 32) || \
    (defined(__GNUC__) && defined(__BYTE_ORDER__) && \
         __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && __SIZEOF_LONG__ == 4)
#define PADDED(x, y)	x; unsigned y
#define PADDEDptr(x, y)	x; unsigned y
#define PADDEDul(x, y)	unsigned long x; unsigned y

/* little endian, 64 bits */
#elif defined(__ia64__) || defined(__x86_64__) || defined(__alpha__) || \
      (defined(__mips64) && defined(__MIPSEL__)) || \
      (defined(__aarch64__) && defined(__AARCH64EL__)) || \
      (defined(__riscv) && __riscv_xlen == 64) || \
      (defined(__GNUC__) && defined(__BYTE_ORDER__) && \
          __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__ && __SIZEOF_LONG__ == 8)
#define PADDED(x, y)	x, y
#define PADDEDptr(x, y)	x
#define PADDEDul(x, y)	unsigned long x

/* big endian, 64 bits */
#elif defined(__powerpc64__) || defined(__s390x__) || \
      (defined(__hppa__) && defined(__arch64__)) || \
      (defined(__sparc__) && defined(__arch64__)) || \
      (defined(__mips64) && defined(__MIPSEB__)) || \
      (defined(__aarch64__) && defined(__AARCH64EB__)) || \
      (defined(__GNUC__) && defined(__BYTE_ORDER__) && \
           __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__ && __SIZEOF_LONG__ == 8)
#define PADDED(x, y)	unsigned y; x
#define PADDEDptr(x,y)	x
#define PADDEDul(x, y)	unsigned long x

/* big endian, 32 bits */
#elif defined(__PPC__) || defined(__s390__) || \
      (defined(__arm__) && defined(__ARMEB__)) || \
      (defined(__sh__) && defined (__BIG_ENDIAN__)) || \
      defined(__sparc__) || defined(__MIPSEB__) || defined(__m68k__) || \
      defined(__hppa__) || defined(__frv__) || defined(__avr32__) || \
      (defined(__GNUC__) && defined(__BYTE_ORDER__) && \
           __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__ && __SIZEOF_LONG__ == 4)
#define PADDED(x, y)	unsigned y; x
#define PADDEDptr(x, y)	unsigned y; x
#define PADDEDul(x, y)	unsigned y; unsigned long x

#else
#error	endian?
#endif

struct io_iocb_poll {
	PADDED(int events, __pad1);
};	/* result code is the set of result flags or -'ve errno */

struct io_iocb_sockaddr {
	struct sockaddr *addr;
	int		len;
};	/* result code is the length of the sockaddr, or -'ve errno */

struct io_iocb_common {
	PADDEDptr(void	*buf, __pad1);
	PADDEDul(nbytes, __pad2);
	long long	offset;
	long long	__pad3;
	unsigned	flags;
	unsigned	resfd;
};	/* result code is the amount read or -'ve errno */

struct io_iocb_vector {
	const struct iovec	*vec;
	int			nr;
	long long		offset;
};	/* result code is the amount read or -'ve errno */

struct iocb {
	PADDEDptr(void *data, __pad1);	/* Return in the io completion event */
	/* key: For use in identifying io requests */
	/* aio_rw_flags: RWF_* flags (such as RWF_NOWAIT) */
	PADDED(unsigned key, aio_rw_flags);

	short		aio_lio_opcode;	
	short		aio_reqprio;
	int		aio_fildes;

	union {
		struct io_iocb_common		c;
		struct io_iocb_vector		v;
		struct io_iocb_poll		poll;
		struct io_iocb_sockaddr	saddr;
	} u;
};

struct io_event {
	PADDEDptr(void *data, __pad1);
	PADDEDptr(struct iocb *obj,  __pad2);
	PADDEDul(res,  __pad3);
	PADDEDul(res2, __pad4);
};

struct io_sigset {
	unsigned long ss;
	unsigned long ss_len;
};

struct io_sigset_compat {
	PADDEDptr(unsigned long ss, __ss_pad);
	unsigned long ss_len;
};

#undef PADDED
#undef PADDEDptr
#undef PADDEDul

typedef void (*io_callback_t)(io_context_t ctx, struct iocb *iocb, long res, long res2);

/* library wrappers */
extern int io_queue_init(int maxevents, io_context_t *ctxp);
/*extern int io_queue_grow(io_context_t ctx, int new_maxevents);*/
extern int io_queue_release(io_context_t ctx);
/*extern int io_queue_wait(io_context_t ctx, struct timespec *timeout);*/
extern int io_queue_run(io_context_t ctx);

/* Actual syscalls */
extern int io_setup(int maxevents, io_context_t *ctxp);
extern int io_destroy(io_context_t ctx);
extern int io_submit(io_context_t ctx, long nr, struct iocb *ios[]);
extern int io_cancel(io_context_t ctx, struct iocb *iocb, struct io_event *evt);
extern int io_getevents(io_context_t ctx, long min_nr, long nr, struct io_event *events, struct timespec *timeout);
extern int io_pgetevents(io_context_t ctx, long min_nr, long nr,
		struct io_event *events, struct timespec *timeout,
		sigset_t *sigmask);


static inline void io_set_callback(struct iocb *iocb, io_callback_t cb)
{
	iocb->data = (void *)cb;
}

static inline void io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PREAD;
	iocb->aio_reqprio = 0;
	iocb->u.c.buf = buf;
	iocb->u.c.nbytes = count;
	iocb->u.c.offset = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PWRITE;
	iocb->aio_reqprio = 0;
	iocb->u.c.buf = buf;
	iocb->u.c.nbytes = count;
	iocb->u.c.offset = offset;
}

static inline void io_prep_preadv(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PREADV;
	iocb->aio_reqprio = 0;
	iocb->u.c.buf = (void *)iov;
	iocb->u.c.nbytes = iovcnt;
	iocb->u.c.offset = offset;
}

static inline void io_prep_pwritev(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PWRITEV;
	iocb->aio_reqprio = 0;
	iocb->u.c.buf = (void *)iov;
	iocb->u.c.nbytes = iovcnt;
	iocb->u.c.offset = offset;
}

static inline void io_prep_preadv2(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset, int flags)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PREADV;
	iocb->aio_reqprio = 0;
	iocb->aio_rw_flags = flags;
	iocb->u.c.buf = (void *)iov;
	iocb->u.c.nbytes = iovcnt;
	iocb->u.c.offset = offset;
}

static inline void io_prep_pwritev2(struct iocb *iocb, int fd, const struct iovec *iov, int iovcnt, long long offset, int flags)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_PWRITEV;
	iocb->aio_reqprio = 0;
	iocb->aio_rw_flags = flags;
	iocb->u.c.buf = (void *)iov;
	iocb->u.c.nbytes = iovcnt;
	iocb->u.c.offset = offset;
}

static inline void io_prep_poll(struct iocb *iocb, int fd, int events)
{
        memset(iocb, 0, sizeof(*iocb));
        iocb->aio_fildes = fd;
        iocb->aio_lio_opcode = IO_CMD_POLL;
        iocb->aio_reqprio = 0;
        iocb->u.poll.events = events;
}

static inline int io_poll(io_context_t ctx, struct iocb *iocb, io_callback_t cb, int fd, int events)
{
        io_prep_poll(iocb, fd, events);
        io_set_callback(iocb, cb);
        return io_submit(ctx, 1, &iocb);
}

static inline void io_prep_fsync(struct iocb *iocb, int fd)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_FSYNC;
	iocb->aio_reqprio = 0;
}

static inline int io_fsync(io_context_t ctx, struct iocb *iocb, io_callback_t cb, int fd)
{
	io_prep_fsync(iocb, fd);
	io_set_callback(iocb, cb);
	return io_submit(ctx, 1, &iocb);
}

static inline void io_prep_fdsync(struct iocb *iocb, int fd)
{
	memset(iocb, 0, sizeof(*iocb));
	iocb->aio_fildes = fd;
	iocb->aio_lio_opcode = IO_CMD_FDSYNC;
	iocb->aio_reqprio = 0;
}

static inline int io_fdsync(io_context_t ctx, struct iocb *iocb, io_callback_t cb, int fd)
{
	io_prep_fdsync(iocb, fd);
	io_set_callback(iocb, cb);
	return io_submit(ctx, 1, &iocb);
}

static inline void io_set_eventfd(struct iocb *iocb, int eventfd)
{
	iocb->u.c.flags |= (1 << 0) /* IOCB_FLAG_RESFD */;
	iocb->u.c.resfd = eventfd;
}

#ifdef __cplusplus
}
#endif

#endif /* __LIBAIO_H */
