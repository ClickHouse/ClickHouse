#include <Common/config.h>

#if USE_LFALLOC
#include "LFAllocator.h"

#include <cstring>
#include <lf_allocX64.h>

namespace DB
{

void * LFAllocator::alloc(size_t size, size_t alignment)
{
    if (alignment == 0)
        return LFAlloc(size);
    else
    {
        void * ptr;
        int res = LFPosixMemalign(&ptr, alignment, size);
        return res ? nullptr : ptr;
    }
}

void LFAllocator::free(void * buf, size_t)
{
    LFFree(buf);
}

void * LFAllocator::realloc(void * old_ptr, size_t, size_t new_size, size_t alignment)
{
    if (old_ptr == nullptr)
    {
        void * result = LFAllocator::alloc(new_size, alignment);
        return result;
    }
    if (new_size == 0)
    {
        LFFree(old_ptr);
        return nullptr;
    }

    void * new_ptr = LFAllocator::alloc(new_size, alignment);
    if (new_ptr == nullptr)
        return nullptr;
    size_t old_size = LFGetSize(old_ptr);
    memcpy(new_ptr, old_ptr, ((old_size < new_size) ? old_size : new_size));
    LFFree(old_ptr);
    return new_ptr;
}

}

#endif
