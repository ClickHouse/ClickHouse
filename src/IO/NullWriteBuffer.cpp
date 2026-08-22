#include <IO/NullWriteBuffer.h>

namespace DB
{

NullWriteBuffer::NullWriteBuffer()
    : WriteBufferFromPointer(data, sizeof(data))
    , write_event(ProfileEvents::end())
{
}

NullWriteBuffer::NullWriteBuffer(const ProfileEvents::Event & write_event_)
    : WriteBufferFromPointer(data, sizeof(data))
    , write_event(write_event_)
{
}

NullWriteBuffer::~NullWriteBuffer()
{
    cancel();
}

void NullWriteBuffer::nextImpl()
{
    // no op, only update counters
    if (write_event != ProfileEvents::end())
        ProfileEvents::increment(write_event, offset());
}

NullWriteBufferWithMemory::NullWriteBufferWithMemory(size_t size)
    : BufferWithOwnMemory(size)
{
}

NullWriteBufferWithMemory::~NullWriteBufferWithMemory()
{
    cancel();
}

}
