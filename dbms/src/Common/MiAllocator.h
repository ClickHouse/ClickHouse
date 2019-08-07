#pragma once

#include <Common/config.h>

#if USE_MIMALLOC
#include <cstddef>

namespace DB
{

/*
 * This is a different allocator that is based on mimalloc (Microsoft malloc).
 * It can be used separately from main allocator to catch heap corruptions and vulnerabilities (for example, for caches).
 * We use MI_SECURE mode in mimalloc to achieve such behaviour.
 */
struct MiAllocator
{
    static void * alloc(size_t size, size_t alignment = 0);

    static void free(void * buf, size_t);

    static void * realloc(void * old_ptr, size_t, size_t new_size, size_t alignment = 0);
};

}

#endif
