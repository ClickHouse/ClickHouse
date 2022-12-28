#include <IO/ConcatSeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

ConcatSeekableReadBuffer::BufferInfo::~BufferInfo()
{
    if (own_in)
        delete in;
}

ConcatSeekableReadBuffer::ConcatSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> buf1, size_t size1, std::unique_ptr<SeekableReadBuffer> buf2, size_t size2) : ConcatSeekableReadBuffer()
{
    appendBuffer(std::move(buf1), size1);
    appendBuffer(std::move(buf2), size2);
}

ConcatSeekableReadBuffer::ConcatSeekableReadBuffer(SeekableReadBuffer & buf1, size_t size1, SeekableReadBuffer & buf2, size_t size2) : ConcatSeekableReadBuffer()
{
    appendBuffer(buf1, size1);
    appendBuffer(buf2, size2);
}

void ConcatSeekableReadBuffer::appendBuffer(std::unique_ptr<SeekableReadBuffer> buffer, size_t size)
{
    appendBuffer(buffer.release(), true, size);
}

void ConcatSeekableReadBuffer::appendBuffer(SeekableReadBuffer & buffer, size_t size)
{
    appendBuffer(&buffer, false, size);
}

void ConcatSeekableReadBuffer::appendBuffer(SeekableReadBuffer * buffer, bool own, size_t size)
{
    BufferInfo info;
    info.in = buffer;
    info.own_in = own;
    info.size = size;

    if (!size)
        return;

    buffers.emplace_back(std::move(info));
    total_size += size;

    if (current == buffers.size() - 1)
    {
        working_buffer = buffers[current].in->buffer();
        pos = buffers[current].in->position();
    }
}

bool ConcatSeekableReadBuffer::nextImpl()
{
    if (current < buffers.size())
    {
        buffers[current].in->position() = pos;
        while ((current < buffers.size()) && buffers[current].in->eof())
        {
            current_start_pos += buffers[current++].size;
            if (current < buffers.size())
                buffers[current].in->seek(0, SEEK_SET);
        }
    }

    if (current >= buffers.size())
    {
        current_start_pos = total_size;
        set(nullptr, 0);
        return false;
    }

    working_buffer = buffers[current].in->buffer();
    pos = buffers[current].in->position();
    return true;
}

off_t ConcatSeekableReadBuffer::getPosition()
{
    size_t current_pos = current_start_pos;
    if (current < buffers.size())
        current_pos += buffers[current].in->getPosition() + offset();
    return current_pos;
}

off_t ConcatSeekableReadBuffer::seek(off_t off, int whence)
{
    off_t new_position;
    off_t current_position = getPosition();
    if (whence == SEEK_SET)
        new_position = off;
    else if (whence == SEEK_CUR)
        new_position = current_position + off;
    else
        throw Exception("ConcatSeekableReadBuffer::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (new_position < 0)
        throw Exception("SEEK_SET underflow: off = " + std::to_string(off), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    if (static_cast<UInt64>(new_position) > total_size)
        throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(new_position) == total_size)
    {
        current = buffers.size();
        current_start_pos = total_size;
        set(nullptr, 0);
        return new_position;
    }

    off_t change_position = new_position - current_position;
    if ((working_buffer.begin() <= pos + change_position) && (pos + change_position <= working_buffer.end()))
    {
        /// Position is still inside the same working buffer.
        pos += change_position;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
        return new_position;
    }

    while (new_position < static_cast<off_t>(current_start_pos))
        current_start_pos -= buffers[--current].size;

    while (new_position >= static_cast<off_t>(current_start_pos + buffers[current].size))
        current_start_pos += buffers[current++].size;

    buffers[current].in->seek(new_position - current_start_pos, SEEK_SET);
    working_buffer = buffers[current].in->buffer();
    pos = buffers[current].in->position();
    return new_position;
}

}
