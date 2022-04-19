#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

#include <zstd.h>

namespace DB
{

/// Performs compression using zstd library and writes compressed data to out_ WriteBuffer.
class ZstdDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    ZstdDeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~ZstdDeflatingWriteBuffer() override;

    void sync() override
    {
        out->sync();
    }

private:
    void nextImpl() override;

    /// Flush all pending data and write zstd footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finalizeBefore() override;
    void finalizeAfter() override;

    ZSTD_CCtx * cctx;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;
};

}
