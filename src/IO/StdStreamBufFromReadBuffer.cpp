#include <IO/StdStreamBufFromReadBuffer.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


StdStreamBufFromReadBuffer::StdStreamBufFromReadBuffer(std::unique_ptr<ReadBuffer> read_buffer_, size_t size_)
    : read_buffer(std::move(read_buffer_)), seekable_read_buffer(dynamic_cast<SeekableReadBuffer *>(read_buffer.get())), size(size_)
{
}

StdStreamBufFromReadBuffer::StdStreamBufFromReadBuffer(ReadBuffer & read_buffer_, size_t size_) : size(size_)
{
    if (dynamic_cast<SeekableReadBuffer *>(&read_buffer_))
    {
        read_buffer = wrapSeekableReadBufferReference(static_cast<SeekableReadBuffer &>(read_buffer_));
        seekable_read_buffer = static_cast<SeekableReadBuffer *>(read_buffer.get());
    }
    else
    {
        read_buffer = wrapReadBufferReference(read_buffer_);
    }
}

StdStreamBufFromReadBuffer::~StdStreamBufFromReadBuffer() = default;

int StdStreamBufFromReadBuffer::underflow()
{
    char c;
    if (!read_buffer->peek(c))
        return std::char_traits<char>::eof();
    return c;
}

std::streamsize StdStreamBufFromReadBuffer::showmanyc()
{
    return read_buffer->available();
}

std::streamsize StdStreamBufFromReadBuffer::xsgetn(char_type* s, std::streamsize count)
{
    return read_buffer->read(s, count);
}

std::streampos StdStreamBufFromReadBuffer::seekoff(std::streamoff off, std::ios_base::seekdir dir, std::ios_base::openmode which)
{
    if (dir == std::ios_base::beg)
        return seekpos(off, which);
    if (dir == std::ios_base::cur)
        return seekpos(getCurrentPosition() + off, which);
    if (dir == std::ios_base::end)
        return seekpos(size + off, which);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong seek's base {}", static_cast<int>(dir));
}

std::streampos StdStreamBufFromReadBuffer::seekpos(std::streampos pos, std::ios_base::openmode which)
{
    if (!(which & std::ios_base::in))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Wrong seek mode {}", static_cast<int>(which));

    std::streamoff offset = pos - getCurrentPosition();
    if (!offset)
        return pos;

    if ((read_buffer->buffer().begin() <= read_buffer->position() + offset) && (read_buffer->position() + offset <= read_buffer->buffer().end()))
    {
        read_buffer->position() += offset;
        return pos;
    }

    if (seekable_read_buffer)
        return seekable_read_buffer->seek(pos, SEEK_SET);

    if (offset > 0)
    {
        read_buffer->ignore(offset);
        return pos;
    }

    throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek's offset {} is out of bound", pos);
}

std::streampos StdStreamBufFromReadBuffer::getCurrentPosition() const
{
    if (seekable_read_buffer)
        return seekable_read_buffer->getPosition();
    return read_buffer->count();
}

std::streamsize StdStreamBufFromReadBuffer::xsputn(const char*, std::streamsize)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "StdStreamBufFromReadBuffer cannot be used for output");
}

int StdStreamBufFromReadBuffer::overflow(int)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "StdStreamBufFromReadBuffer cannot be used for output");
}

}
