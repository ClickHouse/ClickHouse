#include <sys/resource.h>
#include "Stopwatch.h"

StopwatchRUsage::Timestamp StopwatchRUsage::Timestamp::current()
{
    StopwatchRUsage::Timestamp res;

    ::rusage rusage {};
#if !defined(__APPLE__)
#if defined(OS_SUNOS)
    ::getrusage(RUSAGE_LWP, &rusage);
#else
    ::getrusage(RUSAGE_THREAD, &rusage);
#endif // OS_SUNOS
#endif // __APPLE__
    res.user_ns = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec * 1000UL;
    res.sys_ns = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec * 1000UL;
    return res;
}
