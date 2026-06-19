#include <Coordination/ReadBufferFromNuraftBuffer.h>

namespace DB
{

ReadBufferFromNuraftBuffer::ReadBufferFromNuraftBuffer(nuraft::ptr<nuraft::buffer> buffer)
    : ReadBufferFromMemory(buffer->data_begin(), buffer->size())
{
}

ReadBufferFromNuraftBuffer::ReadBufferFromNuraftBuffer(nuraft::buffer & buffer)
    : ReadBufferFromMemory(buffer.data_begin(), buffer.size())
{
}

}
