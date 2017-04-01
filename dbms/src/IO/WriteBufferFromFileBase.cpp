#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

WriteBufferFromFileBase::WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
{
}

WriteBufferFromFileBase::~WriteBufferFromFileBase()
{
}

off_t WriteBufferFromFileBase::seek(off_t off, int whence)
{
    return doSeek(off, whence);
}

void WriteBufferFromFileBase::truncate(off_t length)
{
    return doTruncate(length);
}

}
