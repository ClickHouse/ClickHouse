#include <unistd.h>
#include <syscall.h>
#include "syscall.h"


ssize_t getrandom(void *buf, size_t buflen, unsigned flags)
{
    /// There was cancellable syscall (syscall_cp), but I don't care too.
    return syscall(SYS_getrandom, buf, buflen, flags);
}
