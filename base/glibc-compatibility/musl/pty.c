#include <stdlib.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include "libc.h"
#include "syscall.h"

int posix_openpt(int flags)
{
	return open("/dev/ptmx", flags);
}

int grantpt(int fd)
{
	return 0;
}

int unlockpt(int fd)
{
	int unlock = 0;
	return ioctl(fd, TIOCSPTLCK, &unlock);
}

int __ptsname_r(int fd, char *buf, size_t len)
{
	int pty, err;
	if (!buf) len = 0;
	if ((err = __syscall(SYS_ioctl, fd, TIOCGPTN, &pty))) return -err;
	if (snprintf(buf, len, "/dev/pts/%d", pty) >= len) return ERANGE;
	return 0;
}

weak_alias(__ptsname_r, ptsname_r);
