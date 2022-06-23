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
        if (working_buffer.empty())
        {
            if ((*current)->hasPendingData())
            {
                working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
                return true;
            }
        }
        else
            (*current)->position() = position();

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
    explicit ConcatReadBuffer(const ReadBuffers & buffers_) : ReadBuffer(nullptr, 0), buffers(buffers_), current(buffers.begin())
    {
        assert(!buffers.empty());
    }

    ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2) : ConcatReadBuffer({&buf1, &buf2}) {}
};

}
