#if defined(__APPLE__)
#include <pthread.h>
#elif defined(__FreeBSD__)
#include <pthread.h>
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif

#include <Common/Exception.h>
#include <Common/setThreadName.h>


void setThreadName(const char * name)
{
#if defined(__FreeBSD__)
    pthread_set_name_np(pthread_self(), name);
    return;

#elif defined(__APPLE__)
    if (0 != pthread_setname_np(name))
#else
    if (0 != prctl(PR_SET_NAME, name, 0, 0, 0))
#endif
        DB::throwFromErrno("Cannot set thread name with prctl(PR_SET_NAME...)");
}
