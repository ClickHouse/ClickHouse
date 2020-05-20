#pragma once

#include <Common/Cuda/CudaHostPinnedMemPool.h>

class CudaHostPinnedAllocator
{
public:
    /// Allocate memory range.
    void * alloc(size_t size, size_t alignment = 8)
    {
        return CudaHostPinnedMemPool::instance().alloc(size, alignment);
    }

    /// Free memory range.
    void free(void * buf, size_t size)
    {
        CudaHostPinnedMemPool::instance().free(buf);
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 8)
    {
        return CudaHostPinnedMemPool::instance().realloc(buf, old_size, new_size, alignment);
    }
};
