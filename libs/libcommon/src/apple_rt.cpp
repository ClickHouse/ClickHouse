/** Allows to build on MacOS X
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * To use, include this file with -include compiler parameter.
  */

#include <port/clock.h>

#ifdef __APPLE__
#if !APPLE_HAVE_CLOCK_GETTIME

#include <time.h>
#include <stdlib.h>
#include <mach/mach_init.h>
#include <mach/thread_act.h>
#include <mach/mach_port.h>
#include <sys/time.h>


int clock_gettime_thread(timespec *spec) {
    thread_port_t thread = mach_thread_self();

    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
    thread_basic_info_data_t info;
    if (KERN_SUCCESS != thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count))
        return -1;

    spec->tv_sec = info.user_time.seconds + info.system_time.seconds;
    spec->tv_nsec = info.user_time.microseconds * 1000 + info.system_time.microseconds * 1000;
    mach_port_deallocate(mach_task_self(), thread);

    return 0;
}

int clock_gettime(int clk_id, struct timespec* t) {
    if (clk_id == CLOCK_THREAD_CPUTIME_ID)
        return clock_gettime_thread(t);

    struct timeval now;
    int rv = gettimeofday(&now, NULL);

    if (rv)
        return rv;
    t->tv_sec  = now.tv_sec;
    t->tv_nsec = now.tv_usec * 1000;

    return 0;
}

#endif
#endif
