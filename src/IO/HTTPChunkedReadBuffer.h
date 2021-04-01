#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Reads data with HTTP Chunked Transfer Encoding.
class HTTPChunkedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    HTTPChunkedReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t max_chunk_size) : in(std::move(in_)), max_size(max_chunk_size) {}

private:
    std::unique_ptr<ReadBuffer> in;
    const size_t max_size;

    size_t readChunkHeader();
    void readChunkFooter();

    bool nextImpl() override;
};

}
