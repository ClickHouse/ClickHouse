#include "atomic.h"
#include <sys/auxv.h>
#include <fcntl.h> // open
#include <sys/stat.h> // O_RDONLY
#include <unistd.h> // read, close
#include <stdlib.h> // ssize_t
#include <stdio.h> // perror, fprintf
#include <link.h> // ElfW
#include <errno.h>

#define ARRAY_SIZE(a) sizeof((a))/sizeof((a[0]))

/// Suppress TSan since it is possible for this code to be called from multiple threads,
/// and initialization is safe to be done multiple times from multiple threads.
#if defined(__clang__)
#    define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#else
#    define NO_SANITIZE_THREAD
#endif

// We don't have libc struct available here.
// Compute aux vector manually (from /proc/self/auxv).
//
// Right now there is only 51 AT_* constants,
// so 64 should be enough until this implementation will be replaced with musl.
static unsigned long __auxv_procfs[64];
static unsigned long __auxv_secure = 0;
// Common
static unsigned long * __auxv_environ = NULL;

static void * volatile getauxval_func;

static unsigned long  __auxv_init_environ(unsigned long type);

//
// auxv from procfs interface
//
ssize_t __retry_read(int fd, void * buf, size_t count)
{
    for (;;)
    {
        ssize_t ret = read(fd, buf, count);
        if (ret == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("Cannot read /proc/self/auxv");
            abort();
        }
        return ret;
    }
}
unsigned long NO_SANITIZE_THREAD __getauxval_procfs(unsigned long type)
{
    if (type == AT_SECURE)
    {
        return __auxv_secure;
    }

    if (type >= ARRAY_SIZE(__auxv_procfs))
    {
        errno = ENOENT;
        return 0;
    }

    return __auxv_procfs[type];
}
static unsigned long NO_SANITIZE_THREAD __auxv_init_procfs(unsigned long type)
{
    // For debugging:
    // - od -t dL /proc/self/auxv
    // - LD_SHOW_AUX= ls
    int fd = open("/proc/self/auxv", O_RDONLY);
    // It is possible in case of:
    // - no procfs mounted
    // - on android you are not able to read it unless running from shell or debugging
    // - some other issues
    if (fd == -1)
    {
        // Fallback to environ.
        a_cas_p(&getauxval_func, (void *)__auxv_init_procfs, (void *)__auxv_init_environ);
        return __auxv_init_environ(type);
    }

    ElfW(auxv_t) aux;

    /// NOTE: sizeof(aux) is very small (less then PAGE_SIZE), so partial read should not be possible.
    _Static_assert(sizeof(aux) < 4096, "Unexpected sizeof(aux)");
    while (__retry_read(fd, &aux, sizeof(aux)) == sizeof(aux))
    {
        if (aux.a_type == AT_NULL)
        {
            break;
        }
        if (aux.a_type == AT_IGNORE || aux.a_type == AT_IGNOREPPC)
        {
            continue;
        }

        if (aux.a_type >= ARRAY_SIZE(__auxv_procfs))
        {
            fprintf(stderr, "AT_* is out of range: %li (maximum allowed is %zu)\n", aux.a_type, ARRAY_SIZE(__auxv_procfs));
            abort();
        }
        if (__auxv_procfs[aux.a_type])
        {
            /// It is possible due to race on initialization.
        }
        __auxv_procfs[aux.a_type] = aux.a_un.a_val;
    }
    close(fd);

    __auxv_secure = __getauxval_procfs(AT_SECURE);

    // Now we've initialized __auxv_procfs, next time getauxval() will only call __get_auxval().
    a_cas_p(&getauxval_func, (void *)__auxv_init_procfs, (void *)__getauxval_procfs);

    return __getauxval_procfs(type);
}

//
// auxv from environ interface
//
// NOTE: environ available only after static initializers,
// so you cannot rely on this if you need getauxval() before.
//
// Good example of such user is sanitizers, for example
// LSan will not work with __auxv_init_environ(),
// since it needs getauxval() before.
//
static size_t NO_SANITIZE_THREAD __find_auxv(unsigned long type)
{
    size_t i;
    for (i = 0; __auxv_environ[i]; i += 2)
    {
        if (__auxv_environ[i] == type)
        {
            return i + 1;
        }
    }
    return (size_t) -1;
}
unsigned long NO_SANITIZE_THREAD __getauxval_environ(unsigned long type)
{
    if (type == AT_SECURE)
        return __auxv_secure;

    if (__auxv_environ)
    {
        size_t index = __find_auxv(type);
        if (index != ((size_t) -1))
            return __auxv_environ[index];
    }

    errno = ENOENT;
    return 0;
}
static unsigned long NO_SANITIZE_THREAD __auxv_init_environ(unsigned long type)
{
    if (!__environ)
    {
        // __environ is not initialized yet so we can't initialize __auxv_environ right now.
        // That's normally occurred only when getauxval() is called from some sanitizer's internal code.
        errno = ENOENT;
        return 0;
    }

    // Initialize __auxv_environ and __auxv_secure.
    size_t i;
    for (i = 0; __environ[i]; i++);
    __auxv_environ = (unsigned long *) (__environ + i + 1);

    size_t secure_idx = __find_auxv(AT_SECURE);
    if (secure_idx != ((size_t) -1))
        __auxv_secure = __auxv_environ[secure_idx];

    // Now we need to switch to __getauxval_environ for all later calls, since
    // everything is initialized.
    a_cas_p(&getauxval_func, (void *)__auxv_init_environ, (void *)__getauxval_environ);

    return __getauxval_environ(type);
}

// Callchain:
// - __auxv_init_procfs -> __getauxval_environ
// - __auxv_init_procfs -> __auxv_init_environ -> __getauxval_environ
static void * volatile getauxval_func = (void *)__auxv_init_procfs;

unsigned long getauxval(unsigned long type)
{
    return ((unsigned long (*)(unsigned long))getauxval_func)(type);
}
