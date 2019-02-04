#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#include <brotli/decode.h>

namespace DB {

class BrotliReadBuffer : public BufferWithOwnMemory<ReadBuffer> {
public:
    BrotliReadBuffer(
            ReadBuffer &in_,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char *existing_memory = nullptr,
            size_t alignment = 0);

    ~BrotliReadBuffer() override;

private:
    bool nextImpl() override;

    ReadBuffer &in;
    BrotliDecoderState * bstate_;
    bool eof;
};
}