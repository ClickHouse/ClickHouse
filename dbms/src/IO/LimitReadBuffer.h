#pragma once

#include <IO/ReadBuffer.h>


namespace DB
{

/** Позволяет считать из другого ReadBuffer не более указанного количества байт.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    size_t limit;

    bool nextImpl() override
    {
        if (count() >= limit || !in.next())
            return false;

        working_buffer = in.buffer();
        if (limit - count() < working_buffer.size())
            working_buffer.resize(limit - count());

        return true;
    }

public:
    LimitReadBuffer(ReadBuffer & in_, size_t limit_) : ReadBuffer(nullptr, 0), in(in_), limit(limit_) {}
};

}
