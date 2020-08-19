#include <IO/LimitReadBuffer.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}


bool LimitReadBuffer::nextImpl()
{
    /// Let underlying buffer calculate read bytes in `next()` call.
    in.position() = position();

    if (bytes >= limit)
    {
        if (throw_exception)
            throw Exception("Limit for LimitReadBuffer exceeded: " + exception_message, ErrorCodes::LIMIT_EXCEEDED);
        else
            return false;
    }

    if (!in.next())
        return false;

    working_buffer = in.buffer();

    if (limit - bytes < working_buffer.size())
        working_buffer.resize(limit - bytes);

    return true;
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer & in_, UInt64 limit_, bool throw_exception_, std::string exception_message_)
    : ReadBuffer(in_.position(), 0), in(in_), limit(limit_), throw_exception(throw_exception_), exception_message(std::move(exception_message_))
{
    size_t remaining_bytes_in_buffer = in.buffer().end() - in.position();
    if (remaining_bytes_in_buffer > limit)
        remaining_bytes_in_buffer = limit;

    working_buffer = Buffer(in.position(), in.position() + remaining_bytes_in_buffer);
}


LimitReadBuffer::~LimitReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (working_buffer.size() != 0)
        in.position() = position();
}

}
