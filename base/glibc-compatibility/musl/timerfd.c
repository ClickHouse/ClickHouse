#include <sys/timerfd.h>
#include "syscall.h"

int timerfd_create(int clockid, int flags)
{
    return syscall(SYS_timerfd_create, clockid, flags);
}

int timerfd_settime(int fd, int flags, const struct itimerspec *new, struct itimerspec *old)
{
    return syscall(SYS_timerfd_settime, fd, flags, new, old);
}

int timerfd_gettime(int fd, struct itimerspec *cur)
{
    return syscall(SYS_timerfd_gettime, fd, cur);
}
