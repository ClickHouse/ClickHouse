#pragma once

#include <IO/WriteBuffer.h>


namespace DB
{

/** Writes data with HTTP Chunked Transfer Encoding.
  */
class HTTPChunkedWriteBuffer : public WriteBuffer
{
public:
    HTTPChunkedWriteBuffer(WriteBuffer & out_);

private:
    void setWorkingBuffer();
    void writeChunkHeader(size_t chunk_size);
    void writeChunkFooter();

    void nextImpl() override;
    void finalize() override;

    WriteBuffer & out;
};

}

