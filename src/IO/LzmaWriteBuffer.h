#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

#include <lzma.h>

namespace DB
{
/// Performs compression using lzma library and writes compressed data to out_ WriteBuffer.
class LzmaWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LzmaWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    void finish();

    ~LzmaWriteBuffer() override;

private:
    void nextImpl() override;

    std::unique_ptr<WriteBuffer> out;
    lzma_stream lstr;
    bool finished = false;
};
}