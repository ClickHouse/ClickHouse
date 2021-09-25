#include <IO/PeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PeekableReadBuffer::PeekableReadBuffer(ReadBuffer & sub_buf_, size_t start_size_ /*= DBMS_DEFAULT_BUFFER_SIZE*/)
        : BufferWithOwnMemory(start_size_), sub_buf(sub_buf_)
{
    padded &= sub_buf.isPadded();
    /// Read from sub-buffer
    Buffer & sub_working = sub_buf.buffer();
    BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());

    checkStateCorrect();
}

void PeekableReadBuffer::reset()
{
    checkStateCorrect();

    peeked_size = 0;
    checkpoint = std::nullopt;
    checkpoint_in_own_memory = false;

    if (!currentlyReadFromOwnMemory())
        sub_buf.position() = pos;

    Buffer & sub_working = sub_buf.buffer();
    BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());

    checkStateCorrect();
}

bool PeekableReadBuffer::peekNext()
{
    checkStateCorrect();

    Position copy_from = pos;
    size_t bytes_to_copy = sub_buf.available();
    if (useSubbufferOnly())
    {
        /// Don't have to copy all data from sub-buffer if there is no data in own memory (checkpoint and pos are in sub-buffer)
        if (checkpoint)
            copy_from = *checkpoint;
        bytes_to_copy = sub_buf.buffer().end() - copy_from;
        if (!bytes_to_copy)
        {
            sub_buf.position() = copy_from;

            /// Both checkpoint and pos are at the end of sub-buffer. Just load next part of data.
            bool res = sub_buf.next();
            BufferBase::set(sub_buf.buffer().begin(), sub_buf.buffer().size(), sub_buf.offset());
            if (checkpoint)
                checkpoint.emplace(pos);

            checkStateCorrect();
            return res;
        }
    }

    /// May throw an exception
    resizeOwnMemoryIfNecessary(bytes_to_copy);

    if (useSubbufferOnly())
    {
        sub_buf.position() = copy_from;
    }

    /// Save unread data from sub-buffer to own memory
    memcpy(memory.data() + peeked_size, sub_buf.position(), bytes_to_copy);

    /// If useSubbufferOnly() is false, then checkpoint is in own memory and it was updated in resizeOwnMemoryIfNecessary
    /// Otherwise, checkpoint now at the beginning of own memory
    if (checkpoint && useSubbufferOnly())
    {
        checkpoint.emplace(memory.data());
        checkpoint_in_own_memory = true;
    }

    if (currentlyReadFromOwnMemory())
    {
        /// Update buffer size
        BufferBase::set(memory.data(), peeked_size + bytes_to_copy, offset());
    }
    else
    {
        /// Switch to reading from own memory
        size_t pos_offset = peeked_size + this->offset();
        if (useSubbufferOnly())
        {
            if (checkpoint)
                pos_offset = bytes_to_copy;
            else
                pos_offset = 0;
        }
        BufferBase::set(memory.data(), peeked_size + bytes_to_copy, pos_offset);
    }

    peeked_size += bytes_to_copy;
    sub_buf.position() += bytes_to_copy;

    checkStateCorrect();
    return sub_buf.next();
}

void PeekableReadBuffer::rollbackToCheckpoint(bool drop)
{
    checkStateCorrect();

    assert(checkpoint);

    if (checkpointInOwnMemory() == currentlyReadFromOwnMemory())
    {
        /// Both checkpoint and position are in the same buffer.
        pos = *checkpoint;
    }
    else
    {
        /// Checkpoint is in own memory and position is not.
        assert(checkpointInOwnMemory());

        /// Switch to reading from own memory.
        BufferBase::set(memory.data(), peeked_size, *checkpoint - memory.data());
    }

    if (drop)
        dropCheckpoint();

    checkStateCorrect();
}

bool PeekableReadBuffer::nextImpl()
{
    /// FIXME: wrong bytes count because it can read the same data again after rollbackToCheckpoint()
    ///        however, changing bytes count on every call of next() (even after rollback) allows to determine
    ///        if some pointers were invalidated.

    checkStateCorrect();
    bool res;
    bool checkpoint_at_end = checkpoint && *checkpoint == working_buffer.end() && currentlyReadFromOwnMemory();

    if (checkpoint)
    {
        if (currentlyReadFromOwnMemory())
            res = sub_buf.hasPendingData() || sub_buf.next();
        else
            res = peekNext();
    }
    else
    {
        if (useSubbufferOnly())
        {
            /// Load next data to sub_buf
            sub_buf.position() = position();
            res = sub_buf.next();
        }
        else
        {
            /// All copied data have been read from own memory, continue reading from sub_buf
            peeked_size = 0;
            res = sub_buf.hasPendingData() || sub_buf.next();
        }
    }

    /// Switch to reading from sub_buf (or just update it if already switched)
    Buffer & sub_working = sub_buf.buffer();
    BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());
    nextimpl_working_buffer_offset = sub_buf.offset();

    if (checkpoint_at_end)
    {
        checkpoint.emplace(position());
        peeked_size = 0;
        checkpoint_in_own_memory = false;
    }

    checkStateCorrect();
    return res;
}


void PeekableReadBuffer::checkStateCorrect() const
{
    if (checkpoint)
    {
        if (checkpointInOwnMemory())
        {
            if (!peeked_size)
                throw DB::Exception("Checkpoint in empty own buffer", ErrorCodes::LOGICAL_ERROR);
            if (currentlyReadFromOwnMemory() && pos < *checkpoint)
                throw DB::Exception("Current position in own buffer before checkpoint in own buffer", ErrorCodes::LOGICAL_ERROR);
            if (!currentlyReadFromOwnMemory() && pos < sub_buf.position())
                throw DB::Exception("Current position in subbuffer less than sub_buf.position()", ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            if (peeked_size)
                throw DB::Exception("Own buffer is not empty", ErrorCodes::LOGICAL_ERROR);
            if (currentlyReadFromOwnMemory())
                throw DB::Exception("Current position in own buffer before checkpoint in subbuffer", ErrorCodes::LOGICAL_ERROR);
            if (pos < *checkpoint)
                throw DB::Exception("Current position in subbuffer before checkpoint in subbuffer", ErrorCodes::LOGICAL_ERROR);
        }
    }
    else
    {
        if (!currentlyReadFromOwnMemory() && peeked_size)
            throw DB::Exception("Own buffer is not empty", ErrorCodes::LOGICAL_ERROR);
    }
    if (currentlyReadFromOwnMemory() && !peeked_size)
        throw DB::Exception("Pos in empty own buffer", ErrorCodes::LOGICAL_ERROR);
}

void PeekableReadBuffer::resizeOwnMemoryIfNecessary(size_t bytes_to_append)
{
    checkStateCorrect();
    bool need_update_checkpoint = checkpointInOwnMemory();
    bool need_update_pos = currentlyReadFromOwnMemory();
    size_t offset = 0;
    if (need_update_checkpoint)
        offset = *checkpoint - memory.data();
    else if (need_update_pos)
        offset = this->offset();

    size_t new_size = peeked_size + bytes_to_append;
    if (memory.size() < new_size)
    {
        if (bytes_to_append < offset && 2 * (peeked_size - offset) <= memory.size())
        {
            /// Move unread data to the beginning of own memory instead of resize own memory
            peeked_size -= offset;
            memmove(memory.data(), memory.data() + offset, peeked_size);

            if (need_update_checkpoint)
                *checkpoint -= offset;
            if (need_update_pos)
                pos -= offset;
        }
        else
        {
            size_t pos_offset = pos - memory.data();

            size_t new_size_amortized = memory.size() * 2;
            if (new_size_amortized < new_size)
                new_size_amortized = new_size;
            memory.resize(new_size_amortized);

            if (need_update_checkpoint)
                checkpoint.emplace(memory.data() + offset);
            if (need_update_pos)
            {
                BufferBase::set(memory.data(), peeked_size, pos_offset);
            }
        }
    }
    checkStateCorrect();
}

void PeekableReadBuffer::makeContinuousMemoryFromCheckpointToPos()
{
    if (!checkpoint)
        throw DB::Exception("There is no checkpoint", ErrorCodes::LOGICAL_ERROR);
    checkStateCorrect();

    if (!checkpointInOwnMemory() || currentlyReadFromOwnMemory())
        return;     /// it's already continuous

    size_t bytes_to_append = pos - sub_buf.position();
    resizeOwnMemoryIfNecessary(bytes_to_append);
    memcpy(memory.data() + peeked_size, sub_buf.position(), bytes_to_append);
    sub_buf.position() = pos;
    peeked_size += bytes_to_append;
    BufferBase::set(memory.data(), peeked_size, peeked_size);
}

PeekableReadBuffer::~PeekableReadBuffer()
{
    if (!currentlyReadFromOwnMemory())
        sub_buf.position() = pos;
}

bool PeekableReadBuffer::hasUnreadData() const
{
    return peeked_size && pos != memory.data() + peeked_size;
}

}
