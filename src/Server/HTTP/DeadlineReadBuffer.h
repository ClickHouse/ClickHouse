#pragma once

#include <IO/ReadBuffer.h>

#include <chrono>

namespace DB
{

/// Wraps another ReadBuffer and enforces a wall-clock deadline on reads.
/// Throws Poco::Net::MessageException when the deadline is exceeded,
/// which the HTTP server translates to a 400 Bad Request response.
/// Used to protect HTTP header parsing against slowloris-style attacks.
class DeadlineReadBuffer : public ReadBuffer
{
public:
    using TimePoint = std::chrono::steady_clock::time_point;

    DeadlineReadBuffer(ReadBuffer & in_, TimePoint deadline_)
        : ReadBuffer(in_.position(), 0)
        , in(in_)
        , deadline(deadline_)
    {
        BufferBase::set(in.position(), in.available(), 0);
    }

    ~DeadlineReadBuffer() override
    {
        /// Sync position back to the inner buffer.
        if (!working_buffer.empty())
            in.position() = position();
    }

private:
    bool nextImpl() override;

    ReadBuffer & in;
    TimePoint deadline;
};

}
