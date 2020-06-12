/*
 * Public domain
 *
 * poll(2) emulation for Windows
 *
 * This emulates just-enough poll functionality on Windows to work in the
 * context of the openssl(1) program. This is not a replacement for
 * POSIX.1-2001 poll(2).
 *
 * Dongsheng Song <dongsheng.song@gmail.com>
 * Brent Cook <bcook@openbsd.org>
 */

#ifndef LIBCRYPTOCOMPAT_POLL_H
#define LIBCRYPTOCOMPAT_POLL_H

#ifndef _WIN32
#include_next <poll.h>
#else

#include <winsock2.h>

/* Type used for the number of file descriptors. */
typedef unsigned long int nfds_t;

#if !defined(_WIN32_WINNT) || (_WIN32_WINNT < 0x0600)
/* Data structure describing a polling request. */
struct pollfd {
	int fd; /* file descriptor */
	short events; /* requested events */
	short revents; /* returned events */
};

/* Event types that can be polled */
#define POLLIN 0x001 /* There is data to read. */
#define POLLPRI 0x002 /* There is urgent data to read. */
#define POLLOUT 0x004 /* Writing now will not block. */

# define POLLRDNORM 0x040 /* Normal data may be read. */
# define POLLRDBAND 0x080 /* Priority data may be read. */
# define POLLWRNORM 0x100 /* Writing now will not block. */
# define POLLWRBAND 0x200 /* Priority data may be written. */

/* Event types always implicitly polled. */
#define POLLERR 0x008 /* Error condition. */
#define POLLHUP 0x010 /* Hung up. */
#define POLLNVAL 0x020 /* Invalid polling request. */

#endif

#ifdef __cplusplus
extern "C" {
#endif

int poll(struct pollfd *pfds, nfds_t nfds, int timeout);

#ifdef __cplusplus
}
#endif

#endif /* HAVE_POLL */

#endif /* LIBCRYPTOCOMPAT_POLL_H */
