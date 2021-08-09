#define _DEFAULT_SOURCE
#include <sys/uio.h>
#include <unistd.h>
#include <syscall.h>
#include "syscall.h"

ssize_t pwritev(int fd, const struct iovec *iov, int count, off_t ofs)
{
    /// There was cancellable syscall (syscall_cp), but I don't care.
	return syscall(SYS_pwritev, fd, iov, count, (long)(ofs), (long)(ofs>>32));
}
