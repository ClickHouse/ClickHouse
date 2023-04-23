#define _GNU_SOURCE
#include <fcntl.h>
#include <sys/syscall.h>

extern long int syscall (long int __sysno, ...) __THROW;

int fallocate(int fd, int mode, off_t base, off_t len)
{
	return syscall(SYS_fallocate, fd, mode, base, len);
}
