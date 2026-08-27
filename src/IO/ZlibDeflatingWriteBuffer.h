#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferDecorator.h>


#include <zlib.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_DEFLATE_FAILED;
}

/// Performs compression using zlib library and writes compressed data to out_ WriteBuffer.
class ZlibDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template<typename WriteBufferT>
    ZlibDeflatingWriteBuffer(
            WriteBufferT && out_,
            CompressionMethod compression_method,
            int compression_level,
            size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
            char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
            size_t alignment = 0,
            bool compress_empty_ = true)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment), compress_empty(compress_empty_) /// NOLINT(bugprone-move-forwarding-reference)
    {
        zstr.zalloc = nullptr;
        zstr.zfree = nullptr;
        zstr.opaque = nullptr;
        zstr.next_in = nullptr;
        zstr.avail_in = 0;
        zstr.next_out = nullptr;
        zstr.avail_out = 0;

        int window_bits = 15;
        if (compression_method == CompressionMethod::Gzip)
        {
            window_bits += 16;
        }

        int rc = deflateInit2(&zstr, compression_level, Z_DEFLATED, window_bits, 8, Z_DEFAULT_STRATEGY);

        if (rc != Z_OK)
            throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflateInit2 failed: {}; zlib version: {}", zError(rc), ZLIB_VERSION);
    }

    ~ZlibDeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    /// Flush all pending data and write zlib footer to the underlying buffer.
    /// After the first call to this function, subsequent calls will have no effect and
    /// an attempt to write to this buffer will result in exception.
    void finalizeBefore() override;
    void finalizeAfter() override;

    z_stream zstr;
    bool compress_empty = true;
};

}
