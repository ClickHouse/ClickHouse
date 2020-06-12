/*
 * Public domain
 * sys/select.h compatibility shim
 */

#ifndef _WIN32
#include_next <sys/uio.h>
#else

#include <sys/types.h>

struct iovec {
	void *iov_base;
	size_t iov_len;
};

#endif
