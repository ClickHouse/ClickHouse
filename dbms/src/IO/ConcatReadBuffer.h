#pragma once

#include <vector>

#include <IO/ReadBuffer.h>


namespace DB
{

/** Reads from the concatenation of multiple ReadBuffers
  */
class ConcatReadBuffer : public ReadBuffer
{
public:
    using ReadBuffers = std::vector<ReadBuffer *>;

protected:
    ReadBuffers buffers;
    ReadBuffers::iterator current;

    bool nextImpl() override
    {
        if (buffers.end() == current)
            return false;

        /// First reading
        if (working_buffer.size() == 0 && (*current)->hasPendingData())
        {
            working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
            return true;
        }

        if (!(*current)->next())
        {
            ++current;
            if (buffers.end() == current)
                return false;

            /// We skip the filled up buffers; if the buffer is not filled in, but the cursor is at the end, then read the next piece of data.
            while ((*current)->eof())
            {
                ++current;
                if (buffers.end() == current)
                    return false;
            }
        }

        working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
        return true;
    }

public:
    ConcatReadBuffer(const ReadBuffers & buffers_) : ReadBuffer(nullptr, 0), buffers(buffers_), current(buffers.begin()) {}

    ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2) : ReadBuffer(nullptr, 0)
    {
        buffers.push_back(&buf1);
        buffers.push_back(&buf2);
        current = buffers.begin();
    }
};

}
