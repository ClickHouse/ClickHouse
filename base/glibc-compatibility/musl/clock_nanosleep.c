#include <errno.h>
#include <pthread.h>
#include <time.h>
#include "syscall.h"

int clock_nanosleep(clockid_t clk, int flags, const struct timespec * req, struct timespec * rem)
{
    if (clk == CLOCK_THREAD_CPUTIME_ID)
        return EINVAL;
    int old_cancel_type;
    int status;
    /// We cannot port __syscall_cp because musl has very limited cancellation point implementation.
    /// For example, c++ destructors won't get called and exception unwinding isn't implemented.
    /// Instead, we use normal __syscall here and turn on the asynchrous cancel mode to allow
    /// cancel. This works because nanosleep doesn't contain any resource allocations or
    /// deallocations.
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, &old_cancel_type);
    if (clk == CLOCK_REALTIME && !flags)
        status = -__syscall(SYS_nanosleep, req, rem);
    else
        status = -__syscall(SYS_clock_nanosleep, clk, flags, req, rem);
    pthread_setcanceltype(old_cancel_type, NULL);
    return status;
}
