#include <IO/LimitSeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer & in_, UInt64 start_offset_, UInt64 limit_size_)
    : LimitSeekableReadBuffer(wrapSeekableReadBufferReference(in_), start_offset_, limit_size_)
{
}

LimitSeekableReadBuffer::LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, UInt64 start_offset_, UInt64 limit_size_)
    : SeekableReadBuffer(in_->position(), 0)
    , in(std::move(in_))
    , min_offset(start_offset_)
    , max_offset(start_offset_ + limit_size_)
    , need_seek(start_offset_)
{
}

bool LimitSeekableReadBuffer::nextImpl()
{
    /// First let the nested buffer know the current position in the buffer (otherwise `in->eof()` or `in->seek()` below can work incorrectly).
    in->position() = position();

    if (need_seek)
    {
        /// Do actual seek.
        if (in->getPosition() != *need_seek)
        {
            if (in->seek(*need_seek, SEEK_SET) != static_cast<off_t>(*need_seek))
            {
                /// Failed to seek, maybe because the new seek position is located after EOF.
                set(in->position(), 0);
                return false;
            }
        }
        need_seek.reset();
    }

    if (in->getPosition() >= max_offset)
    {
        /// Limit reached.
        set(in->position(), 0);
        return false;
    }

    if (in->eof())
    {
        /// EOF reached.
        set(in->position(), 0);
        return false;
    }

    /// Adjust the size of the buffer (we don't allow to read more than `max_offset - min_offset`).
    off_t size = in->buffer().size();
    size = std::min(size, max_offset - in->getPosition());

    if (!size || (static_cast<off_t>(in->offset()) >= size))
    {
        /// Limit reached.
        set(in->position(), 0);
        return false;
    }

    BufferBase::set(in->buffer().begin(), size, in->offset());
    return true;
}

off_t LimitSeekableReadBuffer::seek(off_t off, int whence)
{
    off_t new_position;
    off_t current_position = getPosition();
    if (whence == SEEK_SET)
        new_position = off;
    else if (whence == SEEK_CUR)
        new_position = current_position + off;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Seek expects SEEK_SET or SEEK_CUR as whence");

    if (new_position < 0 || new_position + min_offset > max_offset)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Seek shift out of bounds");

    off_t position_change = new_position - current_position;
    if ((buffer().begin() <= pos + position_change) && (pos + position_change <= buffer().end()))
    {
        /// Position is still inside the buffer.
        pos += position_change;
        chassert(pos >= working_buffer.begin());
        chassert(pos <= working_buffer.end());
        return new_position;
    }

    /// Actual seek in the nested buffer will be performed in nextImpl().
    need_seek = new_position + min_offset;

    /// Set the size of the working buffer to zero so next call next() would call nextImpl().
    set(in->position(), 0);

    return new_position;
}

off_t LimitSeekableReadBuffer::getPosition()
{
    if (need_seek)
        return *need_seek - min_offset;

    /// We have to do that because `in->getPosition()` below most likely needs to know the current position in the buffer.
    in->position() = position();

    return in->getPosition() - min_offset;
}

}
