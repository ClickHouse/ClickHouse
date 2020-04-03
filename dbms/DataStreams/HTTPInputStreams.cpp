#include <Common/config.h>
#include <DataStreams/HTTPInputStreams.h>

#include <Poco/Net/HTTPServerRequest.h>

#include <IO/BrotliReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <Compression/CompressedReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_COMPRESSION_METHOD;
}

HTTPInputStreams::HTTPInputStreams(Context & context, HTTPServerRequest & request, HTMLForm & from)
    : in(plainBuffer(request))
    , in_maybe_compressed(compressedBuffer(request, in))
    , in_maybe_internal_compressed(internalCompressedBuffer(from, in_maybe_compressed))
{
    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (context.getSettingsRef().http_native_compression_disable_checksumming_on_decompress)
    {
        if (CompressedReadBuffer * compressed_buffer = typeid_cast<CompressedReadBuffer *>(in_maybe_internal_compressed.get()))
            compressed_buffer->disableChecksumming();
    }
}

std::unique_ptr<ReadBuffer> HTTPInputStreams::plainBuffer(HTTPServerRequest & request) const
{
    return std::make_unique<ReadBufferFromIStream>(request.stream());
}

std::unique_ptr<ReadBuffer> HTTPInputStreams::compressedBuffer(HTTPServerRequest & request, std::unique_ptr<ReadBuffer> & plain_buffer) const
{
    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_compressed_method = request.get("Content-Encoding", "");

    if (!http_compressed_method.empty())
    {
        if (http_compressed_method == "gzip")
            return std::make_unique<ZlibInflatingReadBuffer>(std::move(plain_buffer), CompressionMethod::Gzip);
        else if (http_compressed_method == "deflate")
            return std::make_unique<ZlibInflatingReadBuffer>(std::move(plain_buffer), CompressionMethod::Zlib);
#if USE_BROTLI
        else if (http_compressed_method == "br")
            return std::make_unique<BrotliReadBuffer>(std::move(plain_buffer));
#endif
        else
            throw Exception("Unknown Content-Encoding of HTTP request: " + http_compressed_method, ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    return std::move(plain_buffer);
}

std::unique_ptr<ReadBuffer> HTTPInputStreams::internalCompressedBuffer(
    HTMLForm &params, std::unique_ptr<ReadBuffer> &http_maybe_encoding_buffer) const
{
    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    if (params.getParsed<bool>("decompress", false))
        return std::make_unique<CompressedReadBuffer>(*http_maybe_encoding_buffer);

    return std::move(http_maybe_encoding_buffer);
}

}
