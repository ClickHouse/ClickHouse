#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

class BrotliWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    BrotliWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~BrotliWriteBuffer() override;

    void finalize() override { finish(); }

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    class BrotliStateWrapper;
    std::unique_ptr<BrotliStateWrapper> brotli;

    size_t in_available;
    const uint8_t * in_data;

    size_t out_capacity;
    uint8_t * out_data;

    std::unique_ptr<WriteBuffer> out;

    bool finished = false;
};

}
