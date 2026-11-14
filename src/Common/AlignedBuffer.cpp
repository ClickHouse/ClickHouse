#include <Common/AlignedBuffer.h>

#include <base/alignedAlloc.h>
#include <Common/CacheLine.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/formatReadable.h>
#include <Common/memory.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}


void AlignedBuffer::alloc(size_t size, size_t alignment)
{
    alignment = std::max(alignment, CH_CACHE_LINE_SIZE);
    size = ::Memory::alignUp(size, CH_CACHE_LINE_SIZE);

    void * new_buf = nullptr;
    int res = alignedPosixMemalign(&new_buf, alignment, size);
    if (0 != res)
        throw ErrnoException(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "Cannot allocate memory (aligned allocation), size: {}, alignment: {}.",
            ReadableSize(size),
            ReadableSize(alignment));
    buf = new_buf;
}

void AlignedBuffer::dealloc()
{
    if (buf)
        alignedFree(buf);
}

void AlignedBuffer::reset(size_t size, size_t alignment)
{
    dealloc();
    alloc(size, alignment);
}

AlignedBuffer::AlignedBuffer(size_t size, size_t alignment)
{
    alloc(size, alignment);
}

AlignedBuffer::~AlignedBuffer()
{
    dealloc();
}

}
