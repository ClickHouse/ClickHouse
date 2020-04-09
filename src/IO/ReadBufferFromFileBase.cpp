#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

ReadBufferFromFileBase::ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
{
}

ReadBufferFromFileBase::~ReadBufferFromFileBase()
{
}

off_t ReadBufferFromFileBase::seek(off_t off, int whence)
{
    return doSeek(off, whence);
}

}
