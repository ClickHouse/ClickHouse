#include <IO/PeekableWriteBuffer.h>

namespace DB
{

PeekableWriteBuffer::PeekableWriteBuffer(DB::WriteBuffer & sub_buf_) : BufferWithOwnMemory(0), sub_buf(sub_buf_)
{
    Buffer & sub_working = sub_buf.buffer();
    BufferBase::set(sub_working.begin() + sub_buf.offset(), sub_working.size() - sub_buf.offset(), 0);
}

void PeekableWriteBuffer::nextImpl()
{
    if (checkpoint)
    {
        if (write_to_own_memory)
        {
            size_t prev_size = position() - memory.data();
            size_t new_size = memory.size() * 2;
            memory.resize(new_size);
            BufferBase::set(memory.data() + prev_size, memory.size() - prev_size, 0);
            return;
        }

        if (memory.size() == 0)
            memory.resize(DBMS_DEFAULT_BUFFER_SIZE);

        sub_buf.position() = position();
        BufferBase::set(memory.data(), memory.size(), 0);
        write_to_own_memory = true;
        return;
    }

    sub_buf.position() = position();
    sub_buf.next();
    BufferBase::set(sub_buf.buffer().begin(), sub_buf.buffer().size(), sub_buf.offset());
}


void PeekableWriteBuffer::dropCheckpoint()
{
    assert(checkpoint);
    checkpoint = std::nullopt;

    /// If we have saved data in own memory, write it to sub-buf.
    if (write_to_own_memory)
    {
        try
        {
            sub_buf.next();
            sub_buf.write(memory.data(), position() - memory.data());
            Buffer & sub_working = sub_buf.buffer();
            BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());
            write_to_own_memory = false;
        }
        catch (...)
        {
            /// If exception happened during writing to sub buffer, we should
            /// update buffer to not leave it in invalid state.
            Buffer & sub_working = sub_buf.buffer();
            BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());
            write_to_own_memory = false;
            throw;
        }
    }
}

void PeekableWriteBuffer::rollbackToCheckpoint(bool drop)
{
    assert(checkpoint);

    /// Just ignore all data written after checkpoint.
    if (write_to_own_memory)
    {
        Buffer & sub_working = sub_buf.buffer();
        BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());
        write_to_own_memory = false;
    }

    position() = *checkpoint;

    if (drop)
        checkpoint = std::nullopt;
}

}
