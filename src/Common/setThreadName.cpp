#include <string_view>
#include <unordered_map>
#include "config.h"

#include <pthread.h>

#if defined(OS_DARWIN) || defined(OS_SUNOS)
#elif defined(OS_FREEBSD)
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif

#include <cstring>

#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/Jemalloc.h>

constexpr size_t THREAD_NAME_SIZE = 16;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PTHREAD_ERROR;
}

static_assert(sizeof(enum ThreadName) == 1, "ThreadName enum size is larger than 1 byte");

template<std::size_t N>
constexpr std::string_view as_string_view(char const (&s)[N])
{
    static_assert(N <= THREAD_NAME_SIZE, "Thread name too long");
    return std::string_view(s, N-1); // N-1 does not include null terminator
}

const static std::unordered_map<std::string_view, ThreadName> str_to_thread_name = []{
    std::unordered_map<std::string_view, ThreadName> result;

    #define THREAD_NAME_ACTION(NAME, STR) result[as_string_view(STR)] = ThreadName::NAME;
    THREAD_NAME_VALUES(THREAD_NAME_ACTION)
    #undef THREAD_NAME_ACTION

    return result;
}();

std::string_view toString(ThreadName name)
{
    switch (name)
    {
        case ThreadName::UNKNOWN: return as_string_view("Unknown");

        #define THREAD_NAME_ACTION(NAME, STR) case ThreadName::NAME: return as_string_view(STR);
        THREAD_NAME_VALUES(THREAD_NAME_ACTION)
        #undef THREAD_NAME_ACTION
    }
}

ThreadName parseThreadName(const std::string_view & name)
{
    if (auto it = str_to_thread_name.find(name); it != str_to_thread_name.end())
        return it->second;
    else
        return ThreadName::UNKNOWN;
}

/// Cache thread_name to avoid prctl(PR_GET_NAME) for query_log/text_log
static thread_local ThreadName thread_name = ThreadName::UNKNOWN;

void setThreadName(ThreadName name)
{
    thread_name = name;

    auto thread_name_view = toString(name);
    if (thread_name_view.size() > THREAD_NAME_SIZE)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Thread name cannot be longer than 15 bytes");
    if (thread_name_view.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Thread name cannot be empty");

    // copy string view to string to avoid tidy build error:
    // `call may not be null terminated, provide size information to the callee to prevent potential issues`
    auto thread_name_str = std::string(thread_name_view);

#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), thread_name_str.data());
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(thread_name_str.data()))
#elif defined(OS_SUNOS)
    if (0 != pthread_setname_np(pthread_self(), thread_name_str.data()))
#else
    if (0 != prctl(PR_SET_NAME, thread_name_str.data(), 0, 0, 0))
#endif
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot set thread name with prctl(PR_SET_NAME, ...)");

#if USE_JEMALLOC
    DB::Jemalloc::setValue("thread.prof.name", thread_name_str.data());
#endif
}

ThreadName getThreadName()
{
    if (thread_name != ThreadName::UNKNOWN)
        return thread_name;

    char tmp_thread_name[THREAD_NAME_SIZE] = {};

#if defined(OS_DARWIN) || defined(OS_SUNOS)
    if (pthread_getname_np(pthread_self(), tmp_thread_name, THREAD_NAME_SIZE))
        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_getname_np()");
#elif defined(OS_FREEBSD)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
//        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_get_name_np()");
#else
    if (0 != prctl(PR_GET_NAME, tmp_thread_name, 0, 0, 0))
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with prctl(PR_GET_NAME)");
#endif

    return parseThreadName(std::string_view{tmp_thread_name});
}

}
