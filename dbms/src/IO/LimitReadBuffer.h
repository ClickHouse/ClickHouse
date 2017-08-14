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
    LimitReadBuffer(ReadBuffer & in_, size_t limit_) : ReadBuffer(nullptr, 0), in(in_), limit(limit_) {}
    
    ~LimitReadBuffer() override
    {
        /// Update underlying buffer's position in case when limit wasn't reached.
        if (working_buffer.size() != 0)
            in.position() = position();
    }
};

}
