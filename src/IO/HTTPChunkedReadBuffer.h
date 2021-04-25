#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Reads data with HTTP Chunked Transfer Encoding.
class HTTPChunkedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit HTTPChunkedReadBuffer(std::unique_ptr<ReadBuffer> in_) : in(std::move(in_)) {}

private:
    std::unique_ptr<ReadBuffer> in;

    size_t readChunkHeader();
    void readChunkFooter();

    bool nextImpl() override;
};

}
