#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>

#if USE_ARROW

#include <Common/Exception.h>
#include <cstring>

#include <zstd.h>
#include <lz4frame.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}
}

namespace DB::ArrowIPC
{

PODArray<char> compressBuffer(CompressionCodec codec, const char * src, size_t size, int level)
{
    PODArray<char> out;
    if (codec == CompressionCodec::Zstd)
    {
        const size_t bound = ZSTD_compressBound(size);
        out.resize(bound);
        const size_t n = ZSTD_compress(out.data(), bound, src, size, level);
        if (ZSTD_isError(n))
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "ZSTD compression failed: {}", ZSTD_getErrorName(n));
        out.resize(n);
    }
    else
    {
        LZ4F_preferences_t prefs;
        memset(&prefs, 0, sizeof(prefs));
        prefs.compressionLevel = level;
        const size_t bound = LZ4F_compressFrameBound(size, &prefs);
        out.resize(bound);
        const size_t n = LZ4F_compressFrame(out.data(), bound, src, size, &prefs);
        if (LZ4F_isError(n))
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "LZ4 frame compression failed: {}", LZ4F_getErrorName(n));
        out.resize(n);
    }
    return out;
}

void decompressBuffer(CompressionCodec codec, const char * compressed, size_t compressed_size, char * dst, size_t uncompressed_size)
{
    if (codec == CompressionCodec::Zstd)
    {
        const size_t n = ZSTD_decompress(dst, uncompressed_size, compressed, compressed_size);
        if (ZSTD_isError(n))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "ZSTD decompression failed: {}", ZSTD_getErrorName(n));
        if (n != uncompressed_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "ZSTD produced {} bytes, expected {}", n, uncompressed_size);
        return;
    }

    LZ4F_dctx * dctx = nullptr;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_getVersion())))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot create LZ4 decompression context");

    size_t dst_pos = 0;
    size_t src_pos = 0;
    while (src_pos < compressed_size && dst_pos < uncompressed_size)
    {
        size_t dst_remaining = uncompressed_size - dst_pos;
        size_t src_remaining = compressed_size - src_pos;
        const size_t ret = LZ4F_decompress(dctx, dst + dst_pos, &dst_remaining, compressed + src_pos, &src_remaining, nullptr);
        dst_pos += dst_remaining;
        src_pos += src_remaining;
        if (LZ4F_isError(ret))
        {
            LZ4F_freeDecompressionContext(dctx);
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "LZ4 frame decompression failed: {}", LZ4F_getErrorName(ret));
        }
        if (ret == 0)
            break;
    }
    LZ4F_freeDecompressionContext(dctx);

    if (dst_pos != uncompressed_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "LZ4 produced {} bytes, expected {}", dst_pos, uncompressed_size);
}

}

#endif
