#include <sys/socket.h>
#include <limits.h>
#include <string.h>
#include <errno.h>
#include "syscall.h"

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags)
{
#if LONG_MAX > INT_MAX
	struct msghdr h;
	struct cmsghdr chbuf[1024/sizeof(struct cmsghdr)+1], *c;
	if (msg) {
		h = *msg;
		h.__pad1 = h.__pad2 = 0;
		msg = &h;
		if (h.msg_controllen) {
			if (h.msg_controllen > 1024) {
				errno = ENOMEM;
				return -1;
			}
			memcpy(chbuf, h.msg_control, h.msg_controllen);
			h.msg_control = chbuf;
			for (c=CMSG_FIRSTHDR(&h); c; c=CMSG_NXTHDR(&h,c))
				c->__pad1 = 0;
		}
	}
#endif
	return socketcall_cp(sendmsg, fd, msg, flags, 0, 0, 0);
}
