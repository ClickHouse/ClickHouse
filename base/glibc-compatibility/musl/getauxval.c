#include <sys/auxv.h>
#include "atomic.h"
#include <unistd.h> // __environ
#include <errno.h>

// We don't have libc struct available here. Compute aux vector manually.
static unsigned long * __auxv = NULL;
static unsigned long __auxv_secure = 0;

static size_t __find_auxv(unsigned long type)
{
    size_t i;
    for (i = 0; __auxv[i]; i += 2)
    {
        if (__auxv[i] == type)
            return i + 1;
    }
    return (size_t) -1;
}

unsigned long __getauxval(unsigned long type)
{
    if (type == AT_SECURE)
        return __auxv_secure;

    if (__auxv)
    {
        size_t index = __find_auxv(type);
        if (index != ((size_t) -1))
            return __auxv[index];
    }

    errno = ENOENT;
    return 0;
}

static void * volatile getauxval_func;

static unsigned long  __auxv_init(unsigned long type)
{
    if (!__environ)
    {
        // __environ is not initialized yet so we can't initialize __auxv right now.
        // That's normally occurred only when getauxval() is called from some sanitizer's internal code.
        errno = ENOENT;
        return 0;
    }

    // Initialize __auxv and __auxv_secure.
    size_t i;
    for (i = 0; __environ[i]; i++);
    __auxv = (unsigned long *) (__environ + i + 1);

    size_t secure_idx = __find_auxv(AT_SECURE);
    if (secure_idx != ((size_t) -1))
        __auxv_secure = __auxv[secure_idx];

    // Now we've initialized __auxv, next time getauxval() will only call __get_auxval().
    a_cas_p(&getauxval_func, (void *)__auxv_init, (void *)__getauxval);

    return __getauxval(type);
}

// First time getauxval() will call __auxv_init().
static void * volatile getauxval_func = (void *)__auxv_init;

unsigned long getauxval(unsigned long type)
{
    return ((unsigned long (*)(unsigned long))getauxval_func)(type);
}
