#include <IO/BrotliWriteBuffer.h>

namespace DB
{

BrotliWriteBuffer::BrotliWriteBuffer(
        WriteBuffer & out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0)
        : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
        , out(out_)
{
}

BrotliWriteBuffer::~BrotliWriteBuffer()
{
}

void BrotliWriteBuffer::nextImpl()
{
}

}