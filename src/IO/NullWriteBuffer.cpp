#include <IO/NullWriteBuffer.h>

namespace DB
{

NullWriteBuffer::NullWriteBuffer()
: WriteBufferFromPointer(data, sizeof(data))
{
}

NullWriteBuffer::~NullWriteBuffer()
{
    cancel();
}

void NullWriteBuffer::nextImpl()
{
    // no op
}

}
