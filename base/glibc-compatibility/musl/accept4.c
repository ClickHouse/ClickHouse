#define _GNU_SOURCE
#include <sys/socket.h>
#include <errno.h>
#include <fcntl.h>
#include "syscall.h"

int accept4(int fd, struct sockaddr *restrict addr, socklen_t *restrict len, int flg)
{
	if (!flg) return accept(fd, addr, len);
	int ret = socketcall_cp(accept4, fd, addr, len, flg, 0, 0);
	if (ret>=0 || (errno != ENOSYS && errno != EINVAL)) return ret;
	ret = accept(fd, addr, len);
	if (ret<0) return ret;
	if (flg & SOCK_CLOEXEC)
		__syscall(SYS_fcntl, ret, F_SETFD, FD_CLOEXEC);
	if (flg & SOCK_NONBLOCK)
		__syscall(SYS_fcntl, ret, F_SETFL, O_NONBLOCK);
	return ret;
}
