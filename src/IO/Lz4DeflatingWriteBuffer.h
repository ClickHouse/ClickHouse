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
    template<typename WriteBufferT>
    Lz4DeflatingWriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    , tmp_memory(buf_size)
    , compress_empty(compress_empty_)
    {
        initialize(compression_level);
    }

    ~Lz4DeflatingWriteBuffer() override;

private:
    void initialize(int compression_level);

    void nextImpl() override;

    void finalizeBefore() override;
    void finalizeAfter() override;

    LZ4F_preferences_t kPrefs; /// NOLINT
    LZ4F_compressionContext_t ctx;

    Memory<> tmp_memory;

    bool first_time = true;
    bool compress_empty = true;
};
}
