#include <pthread.h>

#if defined(OS_DARWIN) || defined(OS_SUNOS)
#elif defined(OS_FREEBSD)
    #include <pthread_np.h>
#else
    #include <sys/prctl.h>
#endif

#include <cstring>

#include <Common/Exception.h>
#include <Common/setThreadName.h>

#define THREAD_NAME_SIZE 16


namespace DB
{
namespace ErrorCodes
{
    extern const int PTHREAD_ERROR;
}
}


/// Cache thread_name to avoid prctl(PR_GET_NAME) for query_log/text_log
static thread_local char thread_name[THREAD_NAME_SIZE]{};


void setThreadName(const char * name)
{
#ifndef NDEBUG
    if (strlen(name) > THREAD_NAME_SIZE - 1)
        throw DB::Exception("Thread name cannot be longer than 15 bytes", DB::ErrorCodes::PTHREAD_ERROR);
#endif

#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), name);
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(name))
#elif defined(OS_SUNOS)
    if (0 != pthread_setname_np(pthread_self(), name))
#else
    if (0 != prctl(PR_SET_NAME, name, 0, 0, 0))
#endif
        DB::throwFromErrno("Cannot set thread name with prctl(PR_SET_NAME, ...)", DB::ErrorCodes::PTHREAD_ERROR);

    memcpy(thread_name, name, 1 + strlen(name));
}

const char * getThreadName()
{
    if (thread_name[0])
        return thread_name;

#if defined(OS_DARWIN) || defined(OS_SUNOS)
    if (pthread_getname_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
        throw DB::Exception("Cannot get thread name with pthread_getname_np()", DB::ErrorCodes::PTHREAD_ERROR);
#elif defined(OS_FREEBSD)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
//        throw DB::Exception("Cannot get thread name with pthread_get_name_np()", DB::ErrorCodes::PTHREAD_ERROR);
#else
    if (0 != prctl(PR_GET_NAME, thread_name, 0, 0, 0))
        DB::throwFromErrno("Cannot get thread name with prctl(PR_GET_NAME)", DB::ErrorCodes::PTHREAD_ERROR);
#endif

    return thread_name;
}
