#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferDecorator.h>

namespace DB
{

class BrotliWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    BrotliWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~BrotliWriteBuffer() override;

private:
    void nextImpl() override;

    void finalizeBefore() override;

    class BrotliStateWrapper;
    std::unique_ptr<BrotliStateWrapper> brotli;

    size_t in_available;
    const uint8_t * in_data;

    size_t out_capacity;
    uint8_t * out_data;
};

}
