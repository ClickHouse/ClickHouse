#include "MiAllocator.h"

#if USE_MIMALLOC
#include <mimalloc.h>

#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

void * MiAllocator::alloc(size_t size, size_t alignment)
{
    void * ptr;
    if (alignment == 0)
    {
        ptr = mi_malloc(size);
        if (!ptr)
            DB::throwFromErrno("MiAllocator: Cannot allocate in mimalloc " + formatReadableSizeWithBinarySuffix(size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    else
    {
        ptr = mi_malloc_aligned(size, alignment);
        if (!ptr)
            DB::throwFromErrno("MiAllocator: Cannot allocate in mimalloc (mi_malloc_aligned) " + formatReadableSizeWithBinarySuffix(size) + " with alignment " + toString(alignment) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    return ptr;
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

    void * ptr;

    if (alignment == 0)
    {
        ptr = mi_realloc(old_ptr, alignment);
        if (!ptr)
            DB::throwFromErrno("MiAllocator: Cannot reallocate in mimalloc " + formatReadableSizeWithBinarySuffix(size) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    else
    {
        ptr = mi_realloc_aligned(old_ptr, new_size, alignment);
        if (!ptr)
            DB::throwFromErrno("MiAllocator: Cannot reallocate in mimalloc (mi_realloc_aligned) " + formatReadableSizeWithBinarySuffix(size) + " with alignment " + toString(alignment) + ".", DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);
    }
    return ptr;
}

}

#endif
