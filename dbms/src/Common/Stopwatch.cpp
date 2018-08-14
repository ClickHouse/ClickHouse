#include <sys/resource.h>
#include "Stopwatch.h"

StopWatchRUsage::Timestamp StopWatchRUsage::Timestamp::current()
{
    StopWatchRUsage::Timestamp res;

    ::rusage rusage;
    ::getrusage(RUSAGE_THREAD, &rusage);

    res.user_ns = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec;
    res.sys_ns = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec;
    return res;
}
