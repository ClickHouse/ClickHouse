/// macOS libc guards its time zone state with a private psynch pthread_rwlock (`lcl_rwlock`, Libc
/// `stdtime/FreeBSD/localtime.c`), taken by every function below. It wedges with no holder when its waiters receive
/// signals (FB24027930), and libsystem_c binds it through the two-level namespace, out of reach of
/// pthread_rwlock_shim.c. These definitions serialize the calls made from the binary, so no thread waits on it.

#if defined(__APPLE__)

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <wchar.h>
#include <xlocale.h>

/// Recursive: a call re-entered on the same thread (e.g. from a signal handler) must not self-deadlock here.
static pthread_mutex_t time_zone_mutex = PTHREAD_RECURSIVE_MUTEX_INITIALIZER;

static void * resolveNext(_Atomic(void *) * cache, const char * name)
{
    void * function = atomic_load_explicit(cache, memory_order_acquire);
    if (function)
        return function;

    function = dlsym(RTLD_NEXT, name);
    if (!function)
    {
        static const char prefix[] = "libc time zone shim: cannot resolve ";
        ssize_t unused = write(STDERR_FILENO, prefix, sizeof(prefix) - 1);
        unused = write(STDERR_FILENO, name, strlen(name));
        unused = write(STDERR_FILENO, "\n", 1);
        (void)unused;
        abort();
    }

    atomic_store_explicit(cache, function, memory_order_release);
    return function;
}

/// errno is saved around the mutex: a contended lock or unlock retries an interrupted kernel call and leaves EINTR.
#define WRAP(RET, NAME, PARAMS, ARGS) \
    RET NAME PARAMS \
    { \
        static _Atomic(void *) next; \
        int saved_errno = errno; \
        __typeof__(&NAME) real = (__typeof__(&NAME))resolveNext(&next, #NAME); \
        pthread_mutex_lock(&time_zone_mutex); \
        errno = saved_errno; \
        RET result = real ARGS; \
        saved_errno = errno; \
        pthread_mutex_unlock(&time_zone_mutex); \
        errno = saved_errno; \
        return result; \
    }

#define WRAP_VOID(NAME) \
    void NAME(void) \
    { \
        static _Atomic(void *) next; \
        int saved_errno = errno; \
        __typeof__(&NAME) real = (__typeof__(&NAME))resolveNext(&next, #NAME); \
        pthread_mutex_lock(&time_zone_mutex); \
        errno = saved_errno; \
        real(); \
        saved_errno = errno; \
        pthread_mutex_unlock(&time_zone_mutex); \
        errno = saved_errno; \
    }

WRAP(size_t, strftime,
    (char * __restrict s, size_t maxsize, const char * __restrict format, const struct tm * __restrict timeptr),
    (s, maxsize, format, timeptr))
WRAP(size_t, strftime_l,
    (char * __restrict s, size_t maxsize, const char * __restrict format, const struct tm * __restrict timeptr, locale_t loc),
    (s, maxsize, format, timeptr, loc))
WRAP(size_t, wcsftime,
    (wchar_t * __restrict s, size_t maxsize, const wchar_t * __restrict format, const struct tm * __restrict timeptr),
    (s, maxsize, format, timeptr))
WRAP(size_t, wcsftime_l,
    (wchar_t * __restrict s, size_t maxsize, const wchar_t * __restrict format, const struct tm * __restrict timeptr, locale_t loc),
    (s, maxsize, format, timeptr, loc))
WRAP(char *, strptime,
    (const char * __restrict buf, const char * __restrict format, struct tm * __restrict timeptr),
    (buf, format, timeptr))
WRAP(char *, strptime_l,
    (const char * __restrict buf, const char * __restrict format, struct tm * __restrict timeptr, locale_t loc),
    (buf, format, timeptr, loc))
WRAP(struct tm *, localtime, (const time_t * clock), (clock))
WRAP(struct tm *, localtime_r, (const time_t * __restrict clock, struct tm * __restrict out), (clock, out))
WRAP(char *, ctime, (const time_t * clock), (clock))
WRAP(char *, ctime_r, (const time_t * clock, char * buf), (clock, buf))
WRAP(time_t, mktime, (struct tm * timeptr), (timeptr))
WRAP(time_t, timelocal, (struct tm * const timeptr), (timeptr))
WRAP(time_t, time2posix, (time_t t), (t))
WRAP(time_t, posix2time, (time_t t), (t))
WRAP_VOID(tzset)
WRAP_VOID(tzsetwall)

#endif
