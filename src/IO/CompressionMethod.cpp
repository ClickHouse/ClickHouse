#include <IO/CompressionMethod.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/BrotliReadBuffer.h>
#include <IO/BrotliWriteBuffer.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif


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
        case CompressionMethod::Gzip:   return "gzip";
        case CompressionMethod::Zlib:   return "deflate";
        case CompressionMethod::Brotli: return "br";
        case CompressionMethod::None:   return "";
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

    const std::string * method_str = file_extension.empty() ? &hint : &file_extension;

    if (*method_str == "gzip" || *method_str == "gz")
        return CompressionMethod::Gzip;
    if (*method_str == "deflate")
        return CompressionMethod::Zlib;
    if (*method_str == "brotli" || *method_str == "br")
        return CompressionMethod::Brotli;
    if (hint.empty() || hint == "auto" || hint == "none")
        return CompressionMethod::None;

    throw Exception("Unknown compression method " + hint + ". Only 'auto', 'none', 'gzip', 'br' are supported as compression methods",
        ErrorCodes::NOT_IMPLEMENTED);
}


std::unique_ptr<ReadBuffer> wrapReadBufferWithCompressionMethod(
    std::unique_ptr<ReadBuffer> nested,
    CompressionMethod method,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
{
    if (method == CompressionMethod::Gzip || method == CompressionMethod::Zlib)
        return std::make_unique<ZlibInflatingReadBuffer>(std::move(nested), method, buf_size, existing_memory, alignment);
#if USE_BROTLI
    if (method == CompressionMethod::Brotli)
        return std::make_unique<BrotliReadBuffer>(std::move(nested), buf_size, existing_memory, alignment);
#endif

    if (method == CompressionMethod::None)
        return nested;

    throw Exception("Unsupported compression method", ErrorCodes::NOT_IMPLEMENTED);
}


std::unique_ptr<WriteBuffer> wrapWriteBufferWithCompressionMethod(
    std::unique_ptr<WriteBuffer> nested,
    CompressionMethod method,
    int level,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
{
    if (method == DB::CompressionMethod::Gzip || method == CompressionMethod::Zlib)
        return std::make_unique<ZlibDeflatingWriteBuffer>(std::move(nested), method, level, buf_size, existing_memory, alignment);

#if USE_BROTLI
    if (method == DB::CompressionMethod::Brotli)
        return std::make_unique<BrotliWriteBuffer>(std::move(nested), level, buf_size, existing_memory, alignment);
#endif

    if (method == CompressionMethod::None)
        return nested;

    throw Exception("Unsupported compression method", ErrorCodes::NOT_IMPLEMENTED);
}

}
