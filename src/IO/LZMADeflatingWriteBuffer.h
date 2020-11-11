#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

#include <lzma.h>

namespace DB
{
/// Performs compression using lzma library and writes compressed data to out_ WriteBuffer.
class LZMADeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    LZMADeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    void finish();

    ~LZMADeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    std::unique_ptr<WriteBuffer> out;
    lzma_stream lstr;
    // FL2_CStream * lstr;
    bool finished = false;
};
}
