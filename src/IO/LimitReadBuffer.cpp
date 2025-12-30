#include <limits>
#include <IO/LimitReadBuffer.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>


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

    if (bytes >= settings.read_no_less)
    {
        if (settings.expect_eof && bytes > settings.read_no_more)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Limit for LimitReadBuffer exceeded: {}", settings.excetion_hint);

        if (bytes >= settings.read_no_more)
            return false;

        //throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected data, got {} bytes, expected {}", bytes, settings.read_atmost);
    }

    if (!in->next())
    {
        if (bytes < settings.read_no_less)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Unexpected EOF, got {} of {} bytes", bytes, settings.read_no_less);

        /// Clearing the buffer with existing data.
        BufferBase::set(in->position(), 0, 0);

        return false;
    }

    BufferBase::set(in->position(), std::min(in->available(), getEffectiveBufferSize() - bytes), 0);

    return true;
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer & in_, Settings settings_)
    : ReadBuffer(in_.position(), 0)
    , in(&in_)
    , settings(std::move(settings_))
{
    chassert(in);
    chassert(settings.read_no_less <= settings.read_no_more);

    BufferBase::set(in->position(), std::min(in->available(), getEffectiveBufferSize()), 0);
}


LimitReadBuffer::LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, Settings settings_)
    : LimitReadBuffer(*in_, std::move(settings_))
{
    holder = std::move(in_);
}


LimitReadBuffer::~LimitReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (!working_buffer.empty())
        in->position() = position();
}

size_t LimitReadBuffer::getEffectiveBufferSize() const
{
    if (settings.read_no_less)
        return settings.read_no_less;
    return settings.read_no_more;
}
}
