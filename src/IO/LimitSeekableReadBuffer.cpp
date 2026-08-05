#include <IO/LimitSeekableReadBuffer.h>

#include <Common/Exception.h>

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
    , need_seek(min_offset) /// We always start reading from `min_offset`.
{
}

bool LimitSeekableReadBuffer::nextImpl()
{
    /// First let the nested buffer know the current position in the buffer (otherwise `in->eof()` or `in->seek()` below can work incorrectly).
    in->position() = position();

    if (need_seek)
    {
        /// Do actual seek.
        if (in->seek(*need_seek, SEEK_SET) != static_cast<off_t>(*need_seek))
        {
            /// Failed to seek, maybe because the new seek position is located after EOF.
            set(in->position(), 0);
            return false;
        }
        need_seek.reset();
    }

    off_t seek_pos = in->getPosition();
    off_t offset_after_min = seek_pos - min_offset;
    off_t available_before_max = max_offset - seek_pos;

    if (offset_after_min < 0 || available_before_max <= 0)
    {
        /// Limit reached.
        set(in->position(), 0);
        return false;
    }

    if (in->eof()) /// `in->eof()` can call `in->next()`
    {
        /// EOF reached.
        set(in->position(), 0);
        return false;
    }

    /// in->eof() shouldn't change the seek position.
    chassert(seek_pos == in->getPosition());

    /// Adjust the beginning and the end of the working buffer.
    /// Because we don't want to read before `min_offset` or after `max_offset`.
    auto * ptr = in->position();
    auto * begin = in->buffer().begin();
    auto * end = in->buffer().end();

    if (ptr - begin > offset_after_min)
        begin = ptr - offset_after_min;
    if (end - ptr > available_before_max)
        end = ptr + available_before_max;

    BufferBase::set(begin, end - begin, ptr - begin);
    chassert(position() == ptr && available());

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
