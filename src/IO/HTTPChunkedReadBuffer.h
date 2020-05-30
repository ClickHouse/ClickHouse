#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

/** Reads data with HTTP Chunked Transfer Encoding.
  */
class HTTPChunkedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    HTTPChunkedReadBuffer(ReadBuffer & in_);

private:
    size_t readChunkHeader();
    void readChunkFooter();

    bool nextImpl() override;

    ReadBuffer & in;
};

}
