#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* This assumes that a check for the
   template size has already been made */
static char * __randname(char * template)
{
    int i;
    struct timespec ts;
    unsigned long r;

    clock_gettime(CLOCK_REALTIME, &ts);
    r = (ts.tv_nsec * 65537) ^ ((((intptr_t)(&ts)) / 16) + ((intptr_t)template));
    for (i = 0; i < 6; i++, r >>= 5)
        template[i] = 'A' + (r & 15) + (r & 16) * 2;

    return template;
}

int mkstemps(char * template, int len)
{
    size_t l = strlen(template);
    if (l < 6 || len > l - 6 || memcmp(template + l - len - 6, "XXXXXX", 6))
    {
        errno = EINVAL;
        return -1;
    }

    int fd, retries = 100;
    do
    {
        __randname(template + l - len - 6);
        if ((fd = open(template, O_RDWR | O_CREAT | O_EXCL, 0600)) >= 0)
            return fd;
    } while (--retries && errno == EEXIST);

    memcpy(template + l - len - 6, "XXXXXX", 6);
    return -1;
}
