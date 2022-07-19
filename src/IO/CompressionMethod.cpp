#include <IO/CompressionMethod.h>

#include <IO/BrotliReadBuffer.h>
#include <IO/BrotliWriteBuffer.h>
#include <IO/LZMADeflatingWriteBuffer.h>
#include <IO/LZMAInflatingReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/Bzip2ReadBuffer.h>
#include <IO/Bzip2WriteBuffer.h>
#include <IO/HadoopSnappyReadBuffer.h>

#include <Common/config.h>

#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


std::string toContentEncodingName(CompressionMethod method)
{
    switch (method)
    {
        case CompressionMethod::Gzip:
            return "gzip";
        case CompressionMethod::Zlib:
            return "deflate";
        case CompressionMethod::Brotli:
            return "br";
        case CompressionMethod::Xz:
            return "xz";
        case CompressionMethod::Zstd:
            return "zstd";
        case CompressionMethod::Lz4:
            return "lz4";
        case CompressionMethod::Bzip2:
            return "bz2";
        case CompressionMethod::Snappy:
            return "snappy";
        case CompressionMethod::None:
            return "";
    }
    __builtin_unreachable();
}

CompressionMethod chooseCompressionMethod(const std::string & path, const std::string & hint)
{
    std::string file_extension;
    if (hint.empty() || hint == "auto")
    {
        auto pos = path.find_last_of('.');
        if (pos != std::string::npos)
            file_extension = path.substr(pos + 1, std::string::npos);
    }

    std::string method_str;

    if (file_extension.empty())
        method_str = hint;
    else
        method_str = std::move(file_extension);

    boost::algorithm::to_lower(method_str);

    if (method_str == "gzip" || method_str == "gz")
        return CompressionMethod::Gzip;
    if (method_str == "deflate")
        return CompressionMethod::Zlib;
    if (method_str == "brotli" || method_str == "br")
        return CompressionMethod::Brotli;
    if (method_str == "lzma" || method_str == "xz")
        return CompressionMethod::Xz;
    if (method_str == "zstd" || method_str == "zst")
        return CompressionMethod::Zstd;
    if (method_str == "lz4")
        return CompressionMethod::Lz4;
    if (method_str == "bz2")
        return CompressionMethod::Bzip2;
    if (method_str == "snappy")
        return CompressionMethod::Snappy;
    if (hint.empty() || hint == "auto" || hint == "none")
        return CompressionMethod::None;

    throw Exception(
        "Unknown compression method " + hint + ". Only 'auto', 'none', 'gzip', 'deflate', 'br', 'xz', 'zstd', 'lz4', 'bz2', 'snappy' are supported as compression methods",
        ErrorCodes::NOT_IMPLEMENTED);
}

std::pair<uint64_t, uint64_t> getCompressionLevelRange(const CompressionMethod & method)
{
    switch (method)
    {
        case CompressionMethod::Zstd:
            return {1, 22};
        case CompressionMethod::Lz4:
            return {1, 12};
        default:
            return {1, 9};
    }
}

static std::unique_ptr<CompressedReadBufferWrapper> createCompressedWrapper(
    std::unique_ptr<ReadBuffer> nested, CompressionMethod method, size_t buf_size, char * existing_memory, size_t alignment, int zstd_window_log_max)
{
    if (method == CompressionMethod::Gzip || method == CompressionMethod::Zlib)
        return std::make_unique<ZlibInflatingReadBuffer>(std::move(nested), method, buf_size, existing_memory, alignment);
#if USE_BROTLI
    if (method == CompressionMethod::Brotli)
        return std::make_unique<BrotliReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
#endif
    if (method == CompressionMethod::Xz)
        return std::make_unique<LZMAInflatingReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
    if (method == CompressionMethod::Zstd)
        return std::make_unique<ZstdInflatingReadBuffer>(std::move(nested), buf_size, existing_memory, alignment, zstd_window_log_max);
    if (method == CompressionMethod::Lz4)
        return std::make_unique<Lz4InflatingReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
#if USE_BZIP2
    if (method == CompressionMethod::Bzip2)
        return std::make_unique<Bzip2ReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
#endif
#if USE_SNAPPY
    if (method == CompressionMethod::Snappy)
        return std::make_unique<HadoopSnappyReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
#endif

    throw Exception("Unsupported compression method", ErrorCodes::NOT_IMPLEMENTED);
}

std::unique_ptr<ReadBuffer> wrapReadBufferWithCompressionMethod(
    std::unique_ptr<ReadBuffer> nested, CompressionMethod method, int zstd_window_log_max, size_t buf_size, char * existing_memory, size_t alignment)
{
    if (method == CompressionMethod::None)
        return nested;
    return createCompressedWrapper(std::move(nested), method, buf_size, existing_memory, alignment, zstd_window_log_max);
}

std::unique_ptr<WriteBuffer> wrapWriteBufferWithCompressionMethod(
    std::unique_ptr<WriteBuffer> nested, CompressionMethod method, int level, size_t buf_size, char * existing_memory, size_t alignment)
{
    if (method == DB::CompressionMethod::Gzip || method == CompressionMethod::Zlib)
        return std::make_unique<ZlibDeflatingWriteBuffer>(std::move(nested), method, level, buf_size, existing_memory, alignment);

#if USE_BROTLI
    if (method == DB::CompressionMethod::Brotli)
        return std::make_unique<BrotliWriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);
#endif
    if (method == CompressionMethod::Xz)
        return std::make_unique<LZMADeflatingWriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);

    if (method == CompressionMethod::Zstd)
        return std::make_unique<ZstdDeflatingWriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);

    if (method == CompressionMethod::Lz4)
        return std::make_unique<Lz4DeflatingWriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);

#if USE_BZIP2
    if (method == CompressionMethod::Bzip2)
        return std::make_unique<Bzip2WriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);
#endif
#if USE_SNAPPY
    if (method == CompressionMethod::Snappy)
        throw Exception("Unsupported compression method", ErrorCodes::NOT_IMPLEMENTED);
#endif
    if (method == CompressionMethod::None)
        return nested;

    throw Exception("Unsupported compression method", ErrorCodes::NOT_IMPLEMENTED);
}

}
