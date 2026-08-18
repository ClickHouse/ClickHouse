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
    template<typename WriteBufferT>
    ZstdDeflatingWriteBuffer(
        WriteBufferT && out_,
        int compression_level,
        int window_log = 0,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment), compress_empty(compress_empty_) /// NOLINT(bugprone-move-forwarding-reference)
    {
        initialize(compression_level, window_log);
    }

    ~ZstdDeflatingWriteBuffer() override;

    void sync() override
    {
        out->sync();
    }

private:
    void initialize(int compression_level, int window_log);

    void nextImpl() override;

    /// Flush all pending data and write zstd footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finalizeBefore() override;
    void finalizeAfter() override;

    void flush(ZSTD_EndDirective mode);

    ZSTD_CCtx * cctx;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;

    size_t total_out = 0;
    bool compress_empty = true;
};

}
