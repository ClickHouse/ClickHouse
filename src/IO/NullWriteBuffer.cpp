#include <IO/NullWriteBuffer.h>


namespace DB
{

NullWriteBuffer::NullWriteBuffer(size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
{
}

void NullWriteBuffer::nextImpl()
{
}

}
