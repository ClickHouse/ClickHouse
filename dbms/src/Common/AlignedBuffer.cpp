#include <Common/AlignedBuffer.h>

#include <Common/Exception.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}


void AlignedBuffer::alloc(size_t size, size_t alignment)
{
    void * new_buf;
    int res = ::posix_memalign(&new_buf, std::max(alignment, sizeof(void*)), size);
    if (0 != res)
        throwFromErrno("Cannot allocate memory (posix_memalign), size: "
            + formatReadableSizeWithBinarySuffix(size) + ", alignment: " + formatReadableSizeWithBinarySuffix(alignment) + ".",
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);
    buf = new_buf;
}

void AlignedBuffer::dealloc()
{
    if (buf)
        ::free(buf);
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
