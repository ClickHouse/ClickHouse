#define _GNU_SOURCE
#include <unistd.h>
#include "syscall.h"

ssize_t copy_file_range(int fd_in, off_t *off_in, int fd_out, off_t *off_out, size_t len, unsigned flags)
{
	return syscall(SYS_copy_file_range, fd_in, off_in, fd_out, off_out, len, flags);
}
