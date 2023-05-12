#include <IO/LimitSeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LIMIT_EXCEEDED;
}

bool LimitSeekableReadBuffer::nextImpl()
{
    if (end_position >= static_cast<off_t>(limit))
    {
        /// Limit reached.
        set(in->position(), 0);
        return false;
    }

    assert(position() >= in->position());
    in->position() = position();

    if (!in->next())
    {
        /// EOF reached.
        set(in->position(), 0);
        return false;
    }

    working_buffer = in->buffer();
    pos = in->position();
    end_position = in->getPosition() + in->available();

    if (end_position > static_cast<off_t>(limit))
    {
        working_buffer.resize(working_buffer.size() - end_position + limit);
        end_position = limit;
    }

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
        throw Exception("LimitSeekableReadBuffer::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (new_position < 0)
        throw Exception("SEEK_SET underflow: off = " + std::to_string(off), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    if (static_cast<UInt64>(new_position) > limit)
        throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    off_t change_position = new_position - current_position;
    if ((working_buffer.begin() <= pos + change_position) && (pos + change_position <= working_buffer.end()))
    {
        /// Position is still inside buffer.
        pos += change_position;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
        return new_position;
    }

    in->seek(new_position, SEEK_SET);
    working_buffer = in->buffer();
    pos = in->position();
    end_position = in->getPosition() + in->available();

    if (end_position > static_cast<off_t>(limit))
    {
        working_buffer.resize(working_buffer.size() - end_position + limit);
        end_position = limit;
    }

    return new_position;
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer * in_, bool owns, UInt64 limit_)
    : SeekableReadBuffer(in_ ? in_->position() : nullptr, 0)
    , in(in_)
    , owns_in(owns)
    , limit(limit_)
{
    assert(in);

    off_t current_position = in->getPosition();
    if (current_position > static_cast<off_t>(limit))
        throw Exception("Limit for LimitSeekableReadBuffer exceeded", ErrorCodes::LIMIT_EXCEEDED);

    working_buffer = in->buffer();
    pos = in->position();
    end_position = current_position + in->available();

    if (end_position > static_cast<off_t>(limit))
    {
        working_buffer.resize(working_buffer.size() - end_position + limit);
        end_position = limit;
    }
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer & in_, UInt64 limit_)
    : LimitSeekableReadBuffer(&in_, false, limit_)
{
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, UInt64 limit_)
    : LimitSeekableReadBuffer(in_.release(), true, limit_)
{
}


LimitSeekableReadBuffer::~LimitSeekableReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    in->position() = position();
    if (owns_in)
        delete in;
}

}
