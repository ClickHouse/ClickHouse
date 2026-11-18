#include <sys/epoll.h>
#include <signal.h>
#include <errno.h>
#include "syscall.h"

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#endif

int epoll_create(int size)
{
	return epoll_create1(0);
}

int epoll_create1(int flags)
{
	int r = __syscall(SYS_epoll_create1, flags);
#ifdef SYS_epoll_create
	if (r==-ENOSYS && !flags) r = __syscall(SYS_epoll_create, 1);
#endif
	return __syscall_ret(r);
}

int epoll_ctl(int fd, int op, int fd2, struct epoll_event *ev)
{
	return syscall(SYS_epoll_ctl, fd, op, fd2, ev);
}

int epoll_pwait(int fd, struct epoll_event *ev, int cnt, int to, const sigset_t *sigs)
{
	int r = __syscall(SYS_epoll_pwait, fd, ev, cnt, to, sigs, _NSIG/8);
#ifdef SYS_epoll_wait
	if (r==-ENOSYS && !sigs) r = __syscall(SYS_epoll_wait, fd, ev, cnt, to);
#endif
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
	if (r > 0) __msan_unpoison(ev, r * sizeof(*ev));
#endif
#endif
	return __syscall_ret(r);
}

int epoll_wait(int fd, struct epoll_event *ev, int cnt, int to)
{
	return epoll_pwait(fd, ev, cnt, to, 0);
}
