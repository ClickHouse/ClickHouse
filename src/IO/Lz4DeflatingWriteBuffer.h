#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>


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

    /// TODO: Complete Implementation
};
}
