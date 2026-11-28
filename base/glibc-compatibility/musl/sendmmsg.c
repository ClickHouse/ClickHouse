#define _GNU_SOURCE
#include <sys/socket.h>
#include "syscall.h"

// glibc's struct msghdr matches the kernel layout on x86_64 && aarch64,
// so unlike musl we can pass the vector to the syscall directly
int sendmmsg(int fd, struct mmsghdr *msgvec, unsigned int vlen, int flags)
{
	return syscall(SYS_sendmmsg, fd, msgvec, vlen, flags);
}
