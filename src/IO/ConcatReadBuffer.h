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

    bool nextImpl() override;

public:
    ConcatReadBuffer(const ReadBuffers & buffers_);
    ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2);
};

}
