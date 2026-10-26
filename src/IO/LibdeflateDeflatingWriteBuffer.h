#pragma once

#include "config.h"

#if USE_LIBDEFLATE

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferDecorator.h>

#include <cstdint>

struct libdeflate_compressor;

namespace DB
{

/// Streaming gzip/zlib compressor built on top of libdeflate.
///
/// libdeflate has no native streaming interface. We use libdeflate_deflate_compress_stream_chunk()
/// (a small ClickHouse addition to libdeflate) which compresses each filled buffer into non-final,
/// byte-aligned DEFLATE blocks. We write the gzip/zlib header once, append each chunk's raw DEFLATE
/// bytes, accumulate the checksum, and on finalize append a final empty block and the trailer. The
/// result is a SINGLE valid gzip/zlib member — readable by any standard decoder, including strict
/// ones that stop at the first member (unlike a multi-member stream).
///
/// Memory stays bounded to one buffer. Back-references don't cross buffer boundaries, but with the
/// default buffer size that costs almost nothing (DEFLATE's window is only 32 KiB), and it is
/// outweighed by libdeflate's better per-chunk ratio and speed.
class LibdeflateDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template <typename WriteBufferT>
    LibdeflateDeflatingWriteBuffer(
        WriteBufferT && out_,
        CompressionMethod compression_method,
        int compression_level,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0,
        bool compress_empty_ = true)
        : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
        , compress_empty(compress_empty_)
    {
        init(compression_method, compression_level);
    }

    ~LibdeflateDeflatingWriteBuffer() override;

private:
    void init(CompressionMethod compression_method, int compression_level);

    void nextImpl() override;
    void finalFlushBefore() override;

    void writeHeader();
    void writeTrailer();

    libdeflate_compressor * compressor = nullptr;
    CompressionMethod method = CompressionMethod::None;
    /// CRC32 (gzip) or Adler32 (zlib), updated as data is consumed.
    uint32_t checksum = 0;
    uint64_t total_in = 0;
    bool header_written = false;
    bool compress_empty = true;
    Memory<> scratch;
};

}

#endif
