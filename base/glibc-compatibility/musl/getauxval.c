#include <sys/auxv.h>
#include <unistd.h> // __environ
#include <errno.h>

static size_t __find_auxv(unsigned long type, unsigned long * __auxv)
{
    size_t i;
    for (i = 0; __auxv[i]; i += 2)
    {
        if (__auxv[i] == type)
            return i + 1;
    }
    return (size_t) -1;
}

unsigned long getauxval(unsigned long type)
{
    if (!__environ) /// Not initialized yet
    {
        errno = ENOENT;
        return 0;
    }

    size_t i;
    for (i = 0; __environ[i]; ++i)
        ;
    unsigned long * __auxv = (unsigned long *) (__environ + i + 1);

    if (__auxv)
    {
        size_t index = __find_auxv(type, __auxv);
        if (index != ((size_t) -1))
            return __auxv[index];
    }

    errno = ENOENT;
    return 0;
}
