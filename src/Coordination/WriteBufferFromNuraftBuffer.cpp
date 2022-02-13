#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

void WriteBufferFromNuraftBuffer::nextImpl()
{
    if (finalized)
        throw Exception("WriteBufferFromNuraftBuffer is finished", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

    /// pos may not be equal to vector.data() + old_size, because WriteBuffer::next() can be used to flush data
    size_t pos_offset = pos - reinterpret_cast<Position>(buffer->data_begin());
    size_t old_size = buffer->size();
    if (pos_offset == old_size)
    {
        nuraft::ptr<nuraft::buffer> new_buffer = nuraft::buffer::alloc(old_size * size_multiplier);
        memcpy(new_buffer->data_begin(), buffer->data_begin(), buffer->size());
        buffer = new_buffer;
    }
    internal_buffer = Buffer(reinterpret_cast<Position>(buffer->data_begin() + pos_offset), reinterpret_cast<Position>(buffer->data_begin() + buffer->size()));
    working_buffer = internal_buffer;

}

WriteBufferFromNuraftBuffer::WriteBufferFromNuraftBuffer()
    : WriteBuffer(nullptr, 0)
{
    buffer = nuraft::buffer::alloc(initial_size);
    set(reinterpret_cast<Position>(buffer->data_begin()), buffer->size());
}

void WriteBufferFromNuraftBuffer::finalizeImpl()
{
    size_t real_size = pos - reinterpret_cast<Position>(buffer->data_begin());
    nuraft::ptr<nuraft::buffer> new_buffer = nuraft::buffer::alloc(real_size);
    memcpy(new_buffer->data_begin(), buffer->data_begin(), real_size);
    buffer = new_buffer;

    /// Prevent further writes.
    set(nullptr, 0);
}

nuraft::ptr<nuraft::buffer> WriteBufferFromNuraftBuffer::getBuffer()
{
    finalize();
    return buffer;
}

WriteBufferFromNuraftBuffer::~WriteBufferFromNuraftBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
