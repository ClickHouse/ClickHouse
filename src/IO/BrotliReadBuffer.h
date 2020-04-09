#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

class BrotliReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    BrotliReadBuffer(
            std::unique_ptr<ReadBuffer> in_,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr,
            size_t alignment = 0);

    ~BrotliReadBuffer() override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReadBuffer> in;

    class BrotliStateWrapper;
    std::unique_ptr<BrotliStateWrapper> brotli;

    size_t in_available;
    const uint8_t * in_data;

    size_t out_capacity;
    uint8_t  * out_data;

    bool eof;
};

}

