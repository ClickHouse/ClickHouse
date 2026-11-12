#define _GNU_SOURCE
#include <sys/socket.h>
#include <time.h>
#include "syscall.h"

// glibc's struct msghdr and struct timespec match the kernel layout on x86_64 && aarch64,
// so unlike musl we can pass the arguments to the syscall directly
int recvmmsg(int fd, struct mmsghdr *msgvec, unsigned int vlen, int flags, struct timespec *timeout)
{
	return syscall(SYS_recvmmsg, fd, msgvec, vlen, flags, timeout);
}
