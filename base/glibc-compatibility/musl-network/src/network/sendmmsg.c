#define _GNU_SOURCE
#include <sys/socket.h>
#include <limits.h>
#include <errno.h>
#include "syscall.h"

int sendmmsg(int fd, struct mmsghdr *msgvec, unsigned int vlen, unsigned int flags)
{
#if LONG_MAX > INT_MAX
	/* Can't use the syscall directly because the kernel has the wrong
	 * idea for the types of msg_iovlen, msg_controllen, and cmsg_len,
	 * and the cmsg blocks cannot be modified in-place. */
	int i;
	if (vlen > IOV_MAX) vlen = IOV_MAX; /* This matches the kernel. */
	if (!vlen) return 0;
	for (i=0; i<vlen; i++) {
		/* As an unfortunate inconsistency, the sendmmsg API uses
		 * unsigned int for the resulting msg_len, despite sendmsg
		 * returning ssize_t. However Linux limits the total bytes
		 * sent by sendmsg to INT_MAX, so the assignment is safe. */
		ssize_t r = sendmsg(fd, &msgvec[i].msg_hdr, flags);
		if (r < 0) goto error;
		msgvec[i].msg_len = r;
	}
error:
	return i ? i : -1;
#else
	return syscall_cp(SYS_sendmmsg, fd, msgvec, vlen, flags);
#endif
}
