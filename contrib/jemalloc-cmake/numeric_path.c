/*
 * The interface of the C++ allocator in base/allocator names a mallctl MIB ("Management Information Base": a mallctl
 * name translated into an array of integers) a "numeric path". These functions provide the same names on top of
 * jemalloc, so that ClickHouse works with both allocators.
 */

/* The library exports the `je_` names (see `JEMALLOC_NO_RENAME` in CMakeLists.txt). */
#define JEMALLOC_NO_RENAME
#include <jemalloc/jemalloc.h>

int je_mallctl_name_to_numeric_path(const char * name, size_t * numeric_path, size_t * numeric_path_length)
{
    return je_mallctlnametomib(name, numeric_path, numeric_path_length);
}

int je_mallctl_by_numeric_path(const size_t * numeric_path, size_t numeric_path_length, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return je_mallctlbymib(numeric_path, numeric_path_length, oldp, oldlenp, newp, newlen);
}
