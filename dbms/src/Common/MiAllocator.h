#pragma once

#include <Common/config.h>

#if !USE_MIMALLOC
#error "do not include this file until USE_MIMALLOC is set to 1"
#endif

#include <mimalloc.h>
#include <cstddef>

namespace DB
{
struct MiAllocator
{

    static void * alloc(size_t size, size_t alignment = 0)
    {
        if (alignment == 0)
            return mi_malloc(size);
        else
            return mi_malloc_aligned(size, alignment);
    }

    static void free(void * buf, size_t)
    {
        mi_free(buf);
    }

    static void * realloc(void * old_ptr, size_t, size_t new_size, size_t alignment = 0)
    {
        if (old_ptr == nullptr)
            return alloc(new_size, alignment);

        if (new_size == 0)
        {
            mi_free(old_ptr);
            return nullptr;
        }

        if (alignment == 0)
            return mi_realloc(old_ptr, alignment);

        return mi_realloc_aligned(old_ptr, new_size, alignment);
    }

};

}
