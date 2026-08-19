#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

#include <lzma.h>


namespace DB
{

/// Performs compression using lzma library and writes compressed data to out_ WriteBuffer.
class LZMADeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template<typename WriteBufferT>
    LZMADeflatingWriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment), compress_empty(compress_empty_) /// NOLINT(bugprone-move-forwarding-reference)
    {
        initialize(compression_level);
    }

    ~LZMADeflatingWriteBuffer() override;

private:
    void initialize(int compression_level);

    void nextImpl() override;

    void finalizeBefore() override;
    void finalizeAfter() override;

    lzma_stream lstr;

    bool compress_empty = true;
};

}
