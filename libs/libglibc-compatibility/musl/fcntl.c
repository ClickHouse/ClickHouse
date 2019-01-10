#define _GNU_SOURCE
#include <fcntl.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <syscall.h>
#include "syscall.h"

#include <features.h>
#if defined(__GLIBC__) && __GLIBC__ >= 2
#   define GLIBC_MINOR __GLIBC_MINOR__
#elif defined (__GNU_LIBRARY__) && __GNU_LIBRARY__ >= 2
#   define GLIBC_MINOR __GNU_LIBRARY_MINOR__
#endif

#if defined(GLIBC_MINOR) && GLIBC_MINOR >= 28

int fcntl64(int fd, int cmd, ...)
{
    va_list ap;
    void * arg;

    va_start (ap, cmd);
    arg = va_arg (ap, void *);
    va_end (ap);

    return __syscall(SYS_fcntl, fd, cmd, arg);
}

#endif
