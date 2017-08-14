#pragma once

#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    size_t limit;

    bool nextImpl() override
    {
        /// Let underlying buffer calculate read bytes in `next()` call.
        in.position() = position();

        if (bytes >= limit || !in.next())
            return false;

        working_buffer = in.buffer();

        if (limit - count() < working_buffer.size())
            working_buffer.resize(limit - count());

        return true;
    }

public:
    LimitReadBuffer(ReadBuffer & in_, size_t limit_) : ReadBuffer(in_.position(), 0), in(in_), limit(limit_)
    {
        working_buffer = in.buffer();

        size_t bytes_in_buffer = working_buffer.end() - position();

        working_buffer = Buffer(position(), working_buffer.end());

        if (limit < bytes_in_buffer)
            working_buffer.resize(limit);
    }
    
    virtual ~LimitReadBuffer() override
    {
        /// Update underlying buffer's position in case when limit wasn't reached.
        if (working_buffer.size() != 0)
            in.position() = position();
    }
};

}
