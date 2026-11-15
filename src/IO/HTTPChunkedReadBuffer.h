#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Reads data with HTTP Chunked Transfer Encoding.
class HTTPChunkedReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit HTTPChunkedReadBuffer(std::unique_ptr<ReadBuffer> in_, size_t max_chunk_size_)
        : max_chunk_size(max_chunk_size_), in(std::move(in_))
    {}

    bool poll(size_t timeout_microseconds) override;

private:
    const size_t max_chunk_size;
    std::unique_ptr<ReadBuffer> in;
    bool need_read_chunk_footer = false;

    size_t readChunkHeader();
    void readChunkFooter();

    bool nextImpl() override;
};

}
