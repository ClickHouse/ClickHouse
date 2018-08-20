#include <sys/resource.h>
#include "Stopwatch.h"

StopwatchRUsage::Timestamp StopwatchRUsage::Timestamp::current()
{
    StopwatchRUsage::Timestamp res;

    ::rusage rusage;
    ::getrusage(RUSAGE_THREAD, &rusage);

    res.user_ns = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec * 1000UL;
    res.sys_ns = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec * 1000UL;
    return res;
}
