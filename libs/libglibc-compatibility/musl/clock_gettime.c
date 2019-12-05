#include <errno.h>
#include <stdint.h>
#include <time.h>
#include "atomic.h"
#include "musl_features.h"
#include "syscall.h"

int __clock_gettime(clockid_t clk, struct timespec * ts)
{
    int r;
    r = __syscall(SYS_clock_gettime, clk, ts);
    if (r == -ENOSYS)
    {
        if (clk == CLOCK_REALTIME)
        {
            __syscall(SYS_gettimeofday, ts, 0);
            ts->tv_nsec = (int)ts->tv_nsec * 1000;
            return 0;
        }
        r = -EINVAL;
    }
    return __syscall_ret(r);
}

weak_alias(__clock_gettime, clock_gettime);
