#include <IO/LimitReadBuffer.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int CANNOT_READ_ALL_DATA;
}


bool LimitReadBuffer::nextImpl()
{
    chassert(position() >= in->position());

    /// Let underlying buffer calculate read bytes in `next()` call.
    in->position() = position();

    if (bytes >= limit)
    {
        if (exact_limit && bytes == *exact_limit)
            return false;

        if (exact_limit && bytes != *exact_limit)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected data, got {} bytes, expected {}", bytes, *exact_limit);

        if (throw_exception)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Limit for LimitReadBuffer exceeded: {}", exception_message);

        return false;
    }

    if (!in->next())
    {
        if (exact_limit && bytes != *exact_limit)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected EOF, got {} of {} bytes", bytes, *exact_limit);
        /// Clearing the buffer with existing data.
        BufferBase::set(in->position(), 0, 0);

        return false;
    }

    BufferBase::set(in->position(), std::min(in->available(), limit - bytes), 0);

    return true;
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer * in_, bool owns, size_t limit_, bool throw_exception_,
                                 std::optional<size_t> exact_limit_, std::string exception_message_)
    : ReadBuffer(in_ ? in_->position() : nullptr, 0)
    , in(in_)
    , owns_in(owns)
    , limit(limit_)
    , throw_exception(throw_exception_)
    , exact_limit(exact_limit_)
    , exception_message(std::move(exception_message_))
{
    chassert(in);

    BufferBase::set(in->position(), std::min(in->available(), limit), 0);
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer & in_, size_t limit_, bool throw_exception_,
                                 std::optional<size_t> exact_limit_, std::string exception_message_)
    : LimitReadBuffer(&in_, false, limit_, throw_exception_, exact_limit_, exception_message_)
{
}


LimitReadBuffer::LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t limit_, bool throw_exception_,
                                 std::optional<size_t> exact_limit_, std::string exception_message_)
    : LimitReadBuffer(in_.release(), true, limit_, throw_exception_, exact_limit_, exception_message_)
{
}


LimitReadBuffer::~LimitReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (!working_buffer.empty())
        in->position() = position();

    if (owns_in)
        delete in;
}

}
