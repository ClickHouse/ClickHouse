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

// We don't have libc struct available here.
// Compute aux vector manually (from /proc/self/auxv).
//
// Right now there is only 51 AT_* constants,
// so 64 should be enough until this implementation will be replaced with musl.
static unsigned long __auxv[64];
static unsigned long __auxv_secure = 0;

unsigned long __getauxval(unsigned long type)
{
    if (type == AT_SECURE)
        return __auxv_secure;

    if (type >= ARRAY_SIZE(__auxv))
    {
        errno = ENOENT;
        return 0;
    }

    return __auxv[type];
}

static void * volatile getauxval_func;

ssize_t __retry_read(int fd, void *buf, size_t count)
{
    for (;;)
    {
        ssize_t ret = read(fd, buf, count);
        if (ret == -1)
        {
            if (errno == EINTR)
                continue;
            perror("Cannot read /proc/self/auxv");
            abort();
        }
        return ret;
    }
}
static unsigned long  __auxv_init(unsigned long type)
{
    // od -t dL /proc/self/auxv
    int fd = open("/proc/self/auxv", O_RDONLY);
    if (fd == -1) {
        perror("Cannot read /proc/self/auxv (likely kernel is too old or procfs is not mounted)");
        abort();
    }

    ElfW(auxv_t) aux;

    /// NOTE: sizeof(aux) is very small (less then PAGE_SIZE), so partial read should not be possible.
    _Static_assert(sizeof(aux) < 4096, "Unexpected sizeof(aux)");
    while (__retry_read(fd, &aux, sizeof(aux)) == sizeof(aux))
    {
        if (aux.a_type >= ARRAY_SIZE(__auxv))
        {
            fprintf(stderr, "AT_* is out of range: %li (maximum allowed is %zu)\n", aux.a_type, ARRAY_SIZE(__auxv));
            abort();
        }
        __auxv[aux.a_type] = aux.a_un.a_val;
    }
    close(fd);

    // AT_SECURE
    __auxv_secure = __getauxval(AT_SECURE);

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
