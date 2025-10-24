#include <IO/ConcatReadBufferFromFile.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

ConcatReadBufferFromFile::BufferInfo::BufferInfo(BufferInfo && src) noexcept
    : in(std::exchange(src.in, nullptr)), own_in(std::exchange(src.own_in, false)), size(std::exchange(src.size, 0))
{
}

ConcatReadBufferFromFile::BufferInfo::~BufferInfo()
{
    if (own_in)
        delete in;
}

ConcatReadBufferFromFile::ConcatReadBufferFromFile(std::string file_name_)
    : ReadBufferFromFileBase()
    , file_name(std::move(file_name_))
{
}

ConcatReadBufferFromFile::ConcatReadBufferFromFile(std::string file_name_,
    std::unique_ptr<SeekableReadBuffer> buf1,
    size_t size1,
    std::unique_ptr<SeekableReadBuffer> buf2,
    size_t size2)
    : ReadBufferFromFileBase()
    , file_name(std::move(file_name_))
{
    appendBuffer(std::move(buf1), size1);
    appendBuffer(std::move(buf2), size2);
}

ConcatReadBufferFromFile::ConcatReadBufferFromFile(std::string file_name_,
    SeekableReadBuffer & buf1,
    size_t size1,
    SeekableReadBuffer & buf2,
    size_t size2)
    : ReadBufferFromFileBase()
    , file_name(std::move(file_name_))
{
    appendBuffer(buf1, size1);
    appendBuffer(buf2, size2);
}

void ConcatReadBufferFromFile::appendBuffer(std::unique_ptr<SeekableReadBuffer> buffer, size_t size)
{
    appendBuffer(buffer.release(), true, size);
}

void ConcatReadBufferFromFile::appendBuffer(SeekableReadBuffer & buffer, size_t size)
{
    appendBuffer(&buffer, false, size);
}

void ConcatReadBufferFromFile::appendBuffer(SeekableReadBuffer * buffer, bool own, size_t size)
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

bool ConcatReadBufferFromFile::nextImpl()
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

off_t ConcatReadBufferFromFile::getPosition()
{
    size_t current_pos = current_start_pos;
    if (current < buffers.size())
        current_pos += buffers[current].in->getPosition() + offset();
    return current_pos;
}

off_t ConcatReadBufferFromFile::seek(off_t off, int whence)
{
    off_t new_position;
    off_t current_position = getPosition();
    if (whence == SEEK_SET)
        new_position = off;
    else if (whence == SEEK_CUR)
        new_position = current_position + off;
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "ConcatReadBufferFromFile::seek expects SEEK_SET or SEEK_CUR as whence");

    if (new_position < 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SEEK_SET underflow: off = {}", off);
    if (static_cast<UInt64>(new_position) > total_size)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "SEEK_CUR shift out of bounds");

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
