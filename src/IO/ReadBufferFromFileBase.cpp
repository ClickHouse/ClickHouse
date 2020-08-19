#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

ReadBufferFromFileBase::ReadBufferFromFileBase() : BufferWithOwnMemory<SeekableReadBuffer>(0)
{
}

ReadBufferFromFileBase::ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size, existing_memory, alignment)
{
}

ReadBufferFromFileBase::~ReadBufferFromFileBase() = default;

}
