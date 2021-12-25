#ifndef STDLIB_H
#define STDLIB_H

#include "../../include/stdlib.h"

hidden int __putenv(char *, size_t, char *);
hidden void __env_rm_add(char *, char *);
hidden int __mkostemps(char *, int, int);
hidden int __ptsname_r(int, char *, size_t);
hidden char *__randname(char *);
hidden void __qsort_r (void *, size_t, size_t, int (*)(const void *, const void *, void *), void *);

hidden void *__libc_malloc(size_t);
hidden void *__libc_malloc_impl(size_t);
hidden void *__libc_calloc(size_t, size_t);
hidden void *__libc_realloc(void *, size_t);
hidden void __libc_free(void *);

#endif
