#include <IO/Libdeflate.h>

#if USE_LIBDEFLATE

#include <Common/Exception.h>

#include <libdeflate.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NOT_IMPLEMENTED;
}

namespace Libdeflate
{

namespace
{

bool isGzip(CompressionMethod method)
{
    switch (method)
    {
        case CompressionMethod::Gzip:
            return true;
        case CompressionMethod::Zlib:
            return false;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "libdeflate supports only gzip and zlib (deflate) formats");
    }
}

struct DecompressorDeleter
{
    void operator()(libdeflate_decompressor * d) const noexcept { libdeflate_free_decompressor(d); }
};

struct CompressorDeleter
{
    void operator()(libdeflate_compressor * c) const noexcept { libdeflate_free_compressor(c); }
};

}

void decompress(CompressionMethod method, const char * src, size_t src_size, char * out, size_t uncompressed_size)
{
    const bool gzip = isGzip(method);

    std::unique_ptr<libdeflate_decompressor, DecompressorDeleter> decompressor(libdeflate_alloc_decompressor());
    if (!decompressor)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Failed to allocate libdeflate decompressor");

    size_t actual_out = 0;
    const libdeflate_result result = gzip
        ? libdeflate_gzip_decompress(decompressor.get(), src, src_size, out, uncompressed_size, &actual_out)
        : libdeflate_zlib_decompress(decompressor.get(), src, src_size, out, uncompressed_size, &actual_out);

    switch (result)
    {
        case LIBDEFLATE_SUCCESS:
            break;
        case LIBDEFLATE_BAD_DATA:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Malformed {} stream", gzip ? "gzip" : "zlib");
        case LIBDEFLATE_SHORT_OUTPUT:
        case LIBDEFLATE_INSUFFICIENT_SPACE:
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Decompressed {} stream does not match the expected size of {} bytes",
                gzip ? "gzip" : "zlib", uncompressed_size);
        case LIBDEFLATE_STREAM_NEED_INPUT:
        case LIBDEFLATE_STREAM_NEED_OUTPUT:
            /// Only returned by the streaming API, never by one-shot decompression.
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected libdeflate streaming result in one-shot decompression");
    }

    if (actual_out != uncompressed_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Decompressed size {} does not match the expected size {}", actual_out, uncompressed_size);
}

size_t compressBound(CompressionMethod method, int /*level*/, size_t src_size)
{
    return isGzip(method)
        ? libdeflate_gzip_compress_bound(nullptr, src_size)
        : libdeflate_zlib_compress_bound(nullptr, src_size);
}

size_t compress(CompressionMethod method, int level, const char * src, size_t src_size, char * out, size_t out_capacity)
{
    const bool gzip = isGzip(method);

    /// libdeflate accepts compression levels 1..12.
    if (level < 1 || level > 12)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "libdeflate compression level must be in range [1, 12], got {}", level);

    std::unique_ptr<libdeflate_compressor, CompressorDeleter> compressor(libdeflate_alloc_compressor(level));
    if (!compressor)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Failed to allocate libdeflate compressor for level {}", level);

    const size_t written = gzip
        ? libdeflate_gzip_compress(compressor.get(), src, src_size, out, out_capacity)
        : libdeflate_zlib_compress(compressor.get(), src, src_size, out, out_capacity);

    if (written == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "libdeflate output buffer of {} bytes is too small", out_capacity);

    return written;
}

}

}

#endif
