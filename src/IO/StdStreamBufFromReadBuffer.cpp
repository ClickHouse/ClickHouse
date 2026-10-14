#include <Common/Exception.h>
#include <IO/StdStreamBufFromReadBuffer.h>
#include <IO/SeekableReadBuffer.h>

#include <cstring>


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
    if (gptr() < egptr())
        return std::char_traits<char>::to_int_type(*gptr());

    if (read_buffer->eof())
        return std::char_traits<char>::eof();

    /// Hand the whole buffer of the ReadBuffer over as the get area, so that reading one
    /// character at a time - `std::istreambuf_iterator` over the stream, for example - costs
    /// nothing beyond the first call. The ReadBuffer counts those bytes as read right away;
    /// `getCurrentPosition` and `xsgetn` take the part still in the get area into account.
    char * begin = read_buffer->position();
    char * end = read_buffer->buffer().end();
    read_buffer->position() = end;
    setg(begin, begin, end);

    return std::char_traits<char>::to_int_type(*begin);
}

std::streamsize StdStreamBufFromReadBuffer::showmanyc()
{
    return (egptr() - gptr()) + static_cast<std::streamsize>(read_buffer->available());
}

std::streamsize StdStreamBufFromReadBuffer::xsgetn(char_type* s, std::streamsize count)
{
    std::streamsize copied = 0;

    /// Whatever an earlier `underflow` put into the get area has to be handed out first.
    if (const std::streamsize buffered = egptr() - gptr(); buffered > 0)
    {
        copied = std::min(count, buffered);
        memcpy(s, gptr(), static_cast<size_t>(copied));
        gbump(static_cast<int>(copied));
    }

    if (copied < count)
        copied += static_cast<std::streamsize>(read_buffer->read(s + copied, static_cast<size_t>(count - copied)));

    return copied;
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
    /// This is an input-only stream, so seeking its output side fails the way `std::streambuf`
    /// says it should. Code that hands an `std::iostream` around - the AWS SDK, for example -
    /// does position the output side of a stream it only reads from, and expects a failure, not
    /// an exception.
    if (!(which & std::ios_base::in))
        return std::streampos(std::streamoff(-1));

    std::streamoff offset = pos - getCurrentPosition();
    if (!offset)
        return pos;

    /// Inside the part of the buffer that is already in the get area.
    if (offset > 0 && offset <= egptr() - gptr())
    {
        gbump(static_cast<int>(offset));
        return pos;
    }

    /// Anything else leaves the get area behind; the bytes in it were counted as read.
    offset -= egptr() - gptr();
    setg(nullptr, nullptr, nullptr);

    /// Compare distances rather than pointers: forming `position() + offset` for a target outside
    /// the buffer would already be undefined behavior, even though the seek is about to be rejected.
    const std::streamoff behind_position = read_buffer->position() - read_buffer->buffer().begin();
    const std::streamoff ahead_of_position = read_buffer->buffer().end() - read_buffer->position();

    if ((offset >= -behind_position) && (offset <= ahead_of_position))
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

    throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek's offset {} is out of bound", std::streamoff{pos});
}

std::streampos StdStreamBufFromReadBuffer::getCurrentPosition() const
{
    /// The bytes still in the get area were taken from the ReadBuffer but not read from the
    /// stream yet, so they are behind its position.
    const std::streamoff not_read_yet = egptr() - gptr();

    if (seekable_read_buffer)
        return seekable_read_buffer->getPosition() - not_read_yet;
    return static_cast<std::streamoff>(read_buffer->count()) - not_read_yet;
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
