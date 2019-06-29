#include <Common/config.h>

#if USE_MIMALLOC

#include "MiAllocator.h"
#include <mimalloc.h>

namespace DB
{

void * MiAllocator::alloc(size_t size, size_t alignment)
{
    if (alignment == 0)
        return mi_malloc(size);
    else
        return mi_malloc_aligned(size, alignment);
}

void MiAllocator::free(void * buf, size_t)
{
    mi_free(buf);
}

void * MiAllocator::realloc(void * old_ptr, size_t, size_t new_size, size_t alignment)
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

}

#endif
