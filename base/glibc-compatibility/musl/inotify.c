#include <sys/inotify.h>
#include <errno.h>
#include "syscall.h"

int inotify_init()
{
	return inotify_init1(0);
}
int inotify_init1(int flags)
{
	int r = __syscall(SYS_inotify_init1, flags);
#ifdef SYS_inotify_init
	if (r==-ENOSYS && !flags) r = __syscall(SYS_inotify_init);
#endif
	return __syscall_ret(r);
}

int inotify_add_watch(int fd, const char *pathname, uint32_t mask)
{
	return syscall(SYS_inotify_add_watch, fd, pathname, mask);
}

int inotify_rm_watch(int fd, int wd)
{
	return syscall(SYS_inotify_rm_watch, fd, wd);
}
