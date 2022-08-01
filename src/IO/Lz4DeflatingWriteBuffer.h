#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

#include <lz4.h>
#include <lz4frame.h>

namespace DB
{
/// Performs compression using lz4 library and writes compressed data to out_ WriteBuffer.
class Lz4DeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    Lz4DeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

private:
    void nextImpl() override;

    void finalizeBefore() override;
    void finalizeAfter() override;

    LZ4F_preferences_t kPrefs; /// NOLINT
    LZ4F_compressionContext_t ctx;

    void * in_data;
    void * out_data;

    size_t in_capacity;
    size_t out_capacity;

    bool first_time = true;
};
}
