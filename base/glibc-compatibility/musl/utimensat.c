#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include "syscall.h"
#include <syscall.h>

int utimensat(int fd, const char *path, const struct timespec times[2], int flags)
{
	int r = __syscall(SYS_utimensat, fd, path, times, flags);
#ifdef SYS_futimesat
	if (r != -ENOSYS || flags) return __syscall_ret(r);
	struct timeval *tv = 0, tmp[2];
	if (times) {
		int i;
		tv = tmp;
		for (i=0; i<2; i++) {
			if (times[i].tv_nsec >= 1000000000ULL) {
				if (times[i].tv_nsec == UTIME_NOW &&
				    times[1-i].tv_nsec == UTIME_NOW) {
					tv = 0;
					break;
				}
				if (times[i].tv_nsec == UTIME_OMIT)
					return __syscall_ret(-ENOSYS);
				return __syscall_ret(-EINVAL);
			}
			tmp[i].tv_sec = times[i].tv_sec;
			tmp[i].tv_usec = times[i].tv_nsec / 1000;
		}
	}

	r = __syscall(SYS_futimesat, fd, path, tv);
	if (r != -ENOSYS || fd != AT_FDCWD) return __syscall_ret(r);
	r = __syscall(SYS_utimes, path, tv);
#endif
	return __syscall_ret(r);
}
