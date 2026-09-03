#include <IO/LibdeflateDeflatingWriteBuffer.h>

#if USE_LIBDEFLATE

#include <Common/Exception.h>
#include <IO/Libdeflate.h>

#include <libdeflate.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    /// Standard zlib header (CM=deflate, 32 KiB window, no preset dictionary); FLEVEL is advisory.
    constexpr uint8_t ZLIB_HEADER[2] = {0x78, 0x9c};
    /// Minimal gzip header: magic, CM=deflate, no flags, no mtime, XFL=0, OS=unknown.
    constexpr uint8_t GZIP_HEADER[10] = {0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff};
    /// Empty final block (BFINAL=1, BTYPE=static, end-of-block), used to terminate the stream.
    constexpr uint8_t FINAL_BLOCK[2] = {0x03, 0x00};
}

void LibdeflateDeflatingWriteBuffer::init(CompressionMethod compression_method, int compression_level)
{
    method = compression_method;

    if (method != CompressionMethod::Gzip && method != CompressionMethod::Zlib)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "LibdeflateDeflatingWriteBuffer supports only gzip and zlib formats");

    if (compression_level < 1 || compression_level > 12)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "libdeflate compression level must be in range [1, 12], got {}", compression_level);

    /// Own the compressor through a guard until the throwing tracked allocation below succeeds:
    /// if `scratch.resize` throws (for example on `MEMORY_LIMIT_EXCEEDED`) the constructor unwinds
    /// without running `~LibdeflateDeflatingWriteBuffer`, which would otherwise leak the handle.
    std::unique_ptr<libdeflate_compressor, decltype(&libdeflate_free_compressor)> compressor_holder(
        libdeflate_alloc_compressor(compression_level), &libdeflate_free_compressor);
    if (!compressor_holder)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Failed to allocate libdeflate compressor for level {}", compression_level);

    /// gzip uses CRC32 (init 0), zlib uses Adler32 (init 1).
    checksum = method == CompressionMethod::Gzip ? 0 : 1;

    /// Worst-case output for a full chunk, plus slack for the per-chunk sync flush.
    scratch.resize(libdeflate_deflate_compress_bound(compressor_holder.get(), memory.size()) + 16);

    compressor = compressor_holder.release();
}

LibdeflateDeflatingWriteBuffer::~LibdeflateDeflatingWriteBuffer()
{
    libdeflate_free_compressor(compressor);
}

void LibdeflateDeflatingWriteBuffer::writeHeader()
{
    if (method == CompressionMethod::Gzip)
        out->write(reinterpret_cast<const char *>(GZIP_HEADER), sizeof(GZIP_HEADER));
    else
        out->write(reinterpret_cast<const char *>(ZLIB_HEADER), sizeof(ZLIB_HEADER));
    header_written = true;
}

void LibdeflateDeflatingWriteBuffer::writeTrailer()
{
    if (method == CompressionMethod::Gzip)
    {
        /// CRC32 then ISIZE (input size mod 2^32), both little-endian.
        const uint32_t isize = static_cast<uint32_t>(total_in);
        uint8_t trailer[8];
        for (int i = 0; i < 4; ++i)
            trailer[i] = static_cast<uint8_t>(checksum >> (8 * i));
        for (int i = 0; i < 4; ++i)
            trailer[4 + i] = static_cast<uint8_t>(isize >> (8 * i));
        out->write(reinterpret_cast<const char *>(trailer), sizeof(trailer));
    }
    else
    {
        /// Adler32, big-endian.
        uint8_t trailer[4];
        for (int i = 0; i < 4; ++i)
            trailer[i] = static_cast<uint8_t>(checksum >> (8 * (3 - i)));
        out->write(reinterpret_cast<const char *>(trailer), sizeof(trailer));
    }
}

void LibdeflateDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    try
    {
        if (!header_written)
            writeHeader();

        const char * data = working_buffer.begin();
        const size_t size = offset();

        if (method == CompressionMethod::Gzip)
            checksum = libdeflate_crc32(checksum, data, size);
        else
            checksum = libdeflate_adler32(checksum, data, size);
        total_in += size;

        const size_t bound = libdeflate_deflate_compress_bound(compressor, size) + 16;
        if (scratch.size() < bound)
            scratch.resize(bound);

        const size_t written = libdeflate_deflate_compress_stream_chunk(
            compressor, data, size, scratch.data(), scratch.size());
        if (written == 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "libdeflate failed to compress a {}-byte chunk", size);

        out->write(scratch.data(), written);
    }
    catch (...)
    {
        /// Do not try to write next time after an exception.
        position() = working_buffer.begin();
        throw;
    }
}

void LibdeflateDeflatingWriteBuffer::finalFlushBefore()
{
    next();

    /// No data was ever written: optionally skip producing an empty stream.
    if (!header_written)
    {
        if (!compress_empty)
            return;
        writeHeader();
    }

    out->write(reinterpret_cast<const char *>(FINAL_BLOCK), sizeof(FINAL_BLOCK));
    writeTrailer();
}

}

#endif
