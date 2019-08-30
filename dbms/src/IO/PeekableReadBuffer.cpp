#include <IO/PeekableReadBuffer.h>

namespace DB
{

PeekableReadBuffer::PeekableReadBuffer(ReadBuffer & sub_buf_, size_t start_size_ /*= DBMS_DEFAULT_BUFFER_SIZE*/,
                                                              size_t unread_limit_ /* = default_limit*/)
        : BufferWithOwnMemory(start_size_), sub_buf(sub_buf_), unread_limit(unread_limit_)
{
    padded &= sub_buf.isPadded();
    /// Read from sub-buffer
    Buffer & sub_working = sub_buf.buffer();
    BufferBase::set(sub_working.begin(), sub_working.size(), sub_buf.offset());

    checkStateCorrect();
}

bool PeekableReadBuffer::peekNext()
{
    checkStateCorrect();

    size_t bytes_read = 0;
    Position copy_from = pos;
    size_t bytes_to_copy = sub_buf.available();
    if (useSubbufferOnly())
    {
        /// Don't have to copy all data from sub-buffer if there is no data in own memory (checkpoint and pos are in sub-buffer)
        if (checkpoint)
            copy_from = checkpoint;
        bytes_read = copy_from - sub_buf.buffer().begin();
        bytes_to_copy = sub_buf.buffer().end() - copy_from; /// sub_buf.available();
        if (!bytes_to_copy)
        {
            bytes += bytes_read;
            sub_buf.position() = copy_from;

            /// Both checkpoint and pos are at the end of sub-buffer. Just load next part of data.
            bool res = sub_buf.next();
            BufferBase::set(sub_buf.buffer().begin(), sub_buf.buffer().size(), sub_buf.offset());
            if (checkpoint)
                checkpoint = pos;

            checkStateCorrect();
            return res;
        }
    }

    /// May throw an exception
    resizeOwnMemoryIfNecessary(bytes_to_copy);

    if (useSubbufferOnly())
    {
        bytes += bytes_read;
        sub_buf.position() = copy_from;
    }

    /// Save unread data from sub-buffer to own memory
    memcpy(memory.data() + peeked_size, sub_buf.position(), bytes_to_copy);

    /// If useSubbufferOnly() is false, then checkpoint is in own memory and it was updated in resizeOwnMemoryIfNecessary
    /// Otherwise, checkpoint now at the beginning of own memory
    if (checkpoint && useSubbufferOnly())
    {
        checkpoint = memory.data();
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

void PeekableReadBuffer::setCheckpoint()
{
    checkStateCorrect();
#ifndef NDEBUG
    if (checkpoint)
        throw DB::Exception("Does not support recursive checkpoints.", ErrorCodes::LOGICAL_ERROR);
#endif
    checkpoint_in_own_memory = currentlyReadFromOwnMemory();
    if (!checkpoint_in_own_memory)
    {
        /// Don't need to store unread data anymore
        peeked_size = 0;
    }
    checkpoint = pos;
    checkStateCorrect();
}

void PeekableReadBuffer::dropCheckpoint()
{
    checkStateCorrect();
#ifndef NDEBUG
    if (!checkpoint)
        throw DB::Exception("There is no checkpoint", ErrorCodes::LOGICAL_ERROR);
#endif
    if (!currentlyReadFromOwnMemory())
    {
        /// Don't need to store unread data anymore
        peeked_size = 0;
    }
    checkpoint = nullptr;
    checkpoint_in_own_memory = false;
    checkStateCorrect();
}

void PeekableReadBuffer::rollbackToCheckpoint()
{
    checkStateCorrect();
    if (!checkpoint)
        throw DB::Exception("There is no checkpoint", ErrorCodes::LOGICAL_ERROR);
    else if (checkpointInOwnMemory() == currentlyReadFromOwnMemory())
        pos = checkpoint;
    else /// Checkpoint is in own memory and pos is not. Switch to reading from own memory
        BufferBase::set(memory.data(), peeked_size, checkpoint - memory.data());
    checkStateCorrect();
}

bool PeekableReadBuffer::nextImpl()
{
    /// FIXME wrong bytes count because it can read the same data again after rollbackToCheckpoint()
    /// However, changing bytes count on every call of next() (even after rollback) allows to determine if some pointers were invalidated.
    checkStateCorrect();
    bool res;

    if (!checkpoint)
    {
        if (!useSubbufferOnly())
        {
            /// All copied data have been read from own memory, continue reading from sub_buf
            peeked_size = 0;
            res = sub_buf.hasPendingData() || sub_buf.next();
        }
        else
        {
            /// Load next data to sub_buf
            sub_buf.position() = pos;
            res = sub_buf.next();
        }

        Buffer & sub_working = sub_buf.buffer();
        /// Switch to reading from sub_buf (or just update it if already switched)
        BufferBase::set(sub_working.begin(), sub_working.size(), 0);
    }
    else
    {
        if (currentlyReadFromOwnMemory())
            res = sub_buf.hasPendingData() || sub_buf.next();
        else
            res = peekNext();
        Buffer & sub_working = sub_buf.buffer();
        BufferBase::set(sub_working.begin(), sub_working.size(), 0);
    }

    checkStateCorrect();
    return res;
}

bool PeekableReadBuffer::useSubbufferOnly() const
{
    return !peeked_size;
}

void PeekableReadBuffer::checkStateCorrect() const
{
#ifndef NDEBUG
    if (checkpoint)
    {
        if (checkpointInOwnMemory())
        {
            if (!peeked_size)
                throw DB::Exception("Checkpoint in empty own buffer", ErrorCodes::LOGICAL_ERROR);
            if (currentlyReadFromOwnMemory() && pos < checkpoint)
                throw DB::Exception("Current position in own buffer before checkpoint in own buffer", ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            if (peeked_size)
                throw DB::Exception("Own buffer is not empty", ErrorCodes::LOGICAL_ERROR);
            if (currentlyReadFromOwnMemory())
                throw DB::Exception("Current position in own buffer before checkpoint in subbuffer", ErrorCodes::LOGICAL_ERROR);
            if (pos < checkpoint)
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
    if (unread_limit < memory.size())
        throw DB::Exception("Size limit exceed", ErrorCodes::LOGICAL_ERROR);
#endif
}

size_t PeekableReadBuffer::resizeOwnMemoryIfNecessary(size_t bytes_to_append)
{
    checkStateCorrect();
    bool needUpdateCheckpoint = checkpointInOwnMemory();
    bool needUpdatePos = currentlyReadFromOwnMemory();
    size_t offset = 0;
    if (needUpdateCheckpoint)
        offset = checkpoint - memory.data();
    else if (needUpdatePos)
        offset = this->offset();

    size_t new_size = peeked_size + bytes_to_append;
    if (memory.size() < new_size)
    {
        if (bytes_to_append < offset && 2 * (peeked_size - offset) <= memory.size())
        {
            /// Move unread data to the beginning of own memory instead of resize own memory
            peeked_size -= offset;
            memmove(memory.data(), memory.data() + offset, peeked_size);
            bytes += offset;

            if (needUpdateCheckpoint)
                checkpoint -= offset;
            if (needUpdatePos)
                pos -= offset;

            checkStateCorrect();
            return 0;
        }
        else
        {
            if (unread_limit < new_size)
                throw DB::Exception("PeekableReadBuffer: Memory limit exceed", ErrorCodes::MEMORY_LIMIT_EXCEEDED);

            size_t pos_offset = pos - memory.data();

            size_t new_size_amortized = memory.size() * 2;
            if (new_size_amortized < new_size)
                new_size_amortized = new_size;
            else if (unread_limit < new_size_amortized)
                new_size_amortized = unread_limit;
            memory.resize(new_size_amortized);

            if (needUpdateCheckpoint)
                checkpoint = memory.data() + offset;
            if (needUpdatePos)
            {
                BufferBase::set(memory.data(), peeked_size, pos_offset);
            }
        }
    }

    checkStateCorrect();
    return offset;
}

PeekableReadBuffer::~PeekableReadBuffer()
{
    if (!currentlyReadFromOwnMemory())
        sub_buf.position() = pos;
}

std::shared_ptr<BufferWithOwnMemory<ReadBuffer>> PeekableReadBuffer::takeUnreadData()
{
    checkStateCorrect();
    if (!currentlyReadFromOwnMemory())
        return std::make_shared<BufferWithOwnMemory<ReadBuffer>>(0);
    size_t unread_size = memory.data() + peeked_size - pos;
    auto unread = std::make_shared<BufferWithOwnMemory<ReadBuffer>>(unread_size);
    memcpy(unread->buffer().begin(), pos, unread_size);
    unread->BufferBase::set(unread->buffer().begin(), unread_size, 0);
    peeked_size = 0;
    checkpoint = nullptr;
    checkpoint_in_own_memory = false;
    BufferBase::set(sub_buf.buffer().begin(), sub_buf.buffer().size(), sub_buf.offset());
    checkStateCorrect();
    return unread;
}

bool PeekableReadBuffer::currentlyReadFromOwnMemory() const
{
    return working_buffer.begin() != sub_buf.buffer().begin();
}

bool PeekableReadBuffer::checkpointInOwnMemory() const
{
    return checkpoint_in_own_memory;
}

void PeekableReadBuffer::assertCanBeDestructed() const
{
    if (peeked_size && pos != memory.data() + peeked_size)
        throw DB::Exception("There are data, which were extracted from sub-buffer, but not from peekable buffer. "
                            "Cannot destruct peekable buffer correctly because tha data will be lost."
                            "Most likely it's a bug.", ErrorCodes::LOGICAL_ERROR);
}

}
