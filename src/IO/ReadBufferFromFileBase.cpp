#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FILE_SIZE;
}

ReadBufferFromFileBase::ReadBufferFromFileBase() : BufferWithOwnMemory<SeekableReadBuffer>(0)
{
}

ReadBufferFromFileBase::ReadBufferFromFileBase(
    size_t buf_size,
    char * existing_memory,
    size_t alignment,
    std::optional<size_t> file_size_)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size, existing_memory, alignment)
    , file_size(file_size_)
{
}

ReadBufferFromFileBase::~ReadBufferFromFileBase() = default;

size_t ReadBufferFromFileBase::getFileSize()
{
    if (file_size)
        return *file_size;
    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for read buffer");
}

}
