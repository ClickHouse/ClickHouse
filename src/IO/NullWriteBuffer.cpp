#include <IO/NullWriteBuffer.h>


namespace DB
{

NullWriteBuffer::NullWriteBuffer()
    : WriteBuffer(data, sizeof(data))
{
}

void NullWriteBuffer::nextImpl()
{
}

}
