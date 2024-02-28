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


void setThreadName(const char * name, bool truncate)
{
    size_t name_len = strlen(name);
    if (!truncate && name_len > THREAD_NAME_SIZE - 1)
        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Thread name cannot be longer than 15 bytes");

    size_t name_capped_len = std::min<size_t>(1 + name_len, THREAD_NAME_SIZE - 1);
    char name_capped[THREAD_NAME_SIZE];
    memcpy(name_capped, name, name_capped_len);
    name_capped[name_capped_len] = '\0';

#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), name_capped);
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(name_capped))
#elif defined(OS_SUNOS)
    if (0 != pthread_setname_np(pthread_self(), name_capped))
#else
    if (0 != prctl(PR_SET_NAME, name_capped, 0, 0, 0))
#endif
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot set thread name with prctl(PR_SET_NAME, ...)");

    memcpy(thread_name, name_capped, name_capped_len);
}

const char * getThreadName()
{
    if (thread_name[0])
        return thread_name;

#if defined(OS_DARWIN) || defined(OS_SUNOS)
    if (pthread_getname_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_getname_np()");
#elif defined(OS_FREEBSD)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
//        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_get_name_np()");
#else
    if (0 != prctl(PR_GET_NAME, thread_name, 0, 0, 0))
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with prctl(PR_GET_NAME)");
#endif

    return thread_name;
}
