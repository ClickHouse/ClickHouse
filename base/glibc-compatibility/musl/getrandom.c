/** We have to replace glibc getrandom only when glibc version is higher than 2.25.
 * In previous versions of glibc this function doesn't exist
 * and old kernels may be missing SYS_getrandom syscall.
 */
#include <features.h>
#if defined(__GLIBC__) && __GLIBC__ >= 2
#   define GLIBC_MINOR __GLIBC_MINOR__
#elif defined (__GNU_LIBRARY__) && __GNU_LIBRARY__ >= 2
#   define GLIBC_MINOR __GNU_LIBRARY_MINOR__
#endif

#if defined(GLIBC_MINOR) && GLIBC_MINOR >= 25

#include <unistd.h>
#include <syscall.h>
#include "syscall.h"

ssize_t getrandom(void *buf, size_t buflen, unsigned flags)
{
    /// There was cancellable syscall (syscall_cp), but I don't care too.
    return syscall(SYS_getrandom, buf, buflen, flags);
}
#endif
