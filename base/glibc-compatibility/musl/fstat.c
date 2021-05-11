#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include "syscall.h"

#define AT_EMPTY_PATH 0x1000

int fstat(int fd, struct stat *st)
{
	if (fd<0) return __syscall_ret(-EBADF);
	return fstatat(fd, "", st, AT_EMPTY_PATH);
}
