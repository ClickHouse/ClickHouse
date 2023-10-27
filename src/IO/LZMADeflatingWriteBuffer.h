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
        char * existing_memory = nullptr,
        size_t alignment = 0)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment)
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
};

}
