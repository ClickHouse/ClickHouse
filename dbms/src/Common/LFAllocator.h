#pragma once

#include <Common/config.h>

#if !USE_LFALLOC
#error "do not include this file until USE_LFALLOC is set to 1"
#endif

#include <cstddef>

namespace DB
{
struct LFAllocator
{
    static void * alloc(size_t size, size_t alignment = 0);

    static void free(void * buf, size_t);

    static void * realloc(void * buf, size_t, size_t new_size, size_t alignment = 0);
};

}
