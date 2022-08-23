#define _GNU_SOURCE
#include <fcntl.h>
#include "syscall.h"

long splice(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned flags)
{
	return syscall(SYS_splice, fd_in, off_in, fd_out, off_out, len, flags);
}
