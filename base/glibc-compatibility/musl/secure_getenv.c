#define _GNU_SOURCE
#include <stdlib.h>
#include <sys/auxv.h>

char * secure_getenv(const char * name)
{
    return getauxval(AT_SECURE) ? NULL : getenv(name);
}
