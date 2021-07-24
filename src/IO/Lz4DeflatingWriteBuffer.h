#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>

#include <lz4.h>
#include <lz4frame.h>

namespace DB
{
/// Performs compression using lz4 library and writes compressed data to out_ WriteBuffer.
class Lz4DeflatingWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    Lz4DeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    void finalize() override { finish(); }

    ~Lz4DeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    void finish();
    void finishImpl();

    std::unique_ptr<WriteBuffer> out;

    bool finished = false;


    void * in_buff;
    void * out_buff;
    size_t in_chunk_size;
    size_t out_capacity;

    LZ4F_compressionContext_t ctx;

    size_t compression_ctx;

    uint64_t count_in;
    uint64_t count_out;

    LZ4F_preferences_t kPrefs;
};
}
