#include <Interpreters/CustomHTTP/HTTPInputStreams.h>

#include <Poco/Net/HTTPServerRequest.h>

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
    : in(createRawInBuffer(request))
    , in_maybe_compressed(createCompressedBuffer(request, in))
    , in_maybe_internal_compressed(createInternalCompressedBuffer(from, in_maybe_compressed))
{
    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (context.getSettingsRef().http_native_compression_disable_checksumming_on_decompress)
    {
        if (CompressedReadBuffer * compressed_buffer = typeid_cast<CompressedReadBuffer *>(in_maybe_internal_compressed.get()))
            compressed_buffer->disableChecksumming();
    }
}

ReadBufferPtr HTTPInputStreams::createRawInBuffer(HTTPServerRequest & request) const
{
    return std::make_unique<ReadBufferFromIStream>(request.stream());
}

ReadBufferPtr HTTPInputStreams::createCompressedBuffer(HTTPServerRequest & request, ReadBufferPtr & raw_buffer) const
{
    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_compressed_method = request.get("Content-Encoding", "");

    if (!http_compressed_method.empty())
    {
        if (http_compressed_method == "gzip")
            return std::make_shared<ZlibInflatingReadBuffer>(*raw_buffer, CompressionMethod::Gzip);
        else if (http_compressed_method == "deflate")
            return std::make_shared<ZlibInflatingReadBuffer>(*raw_buffer, CompressionMethod::Zlib);
#if USE_BROTLI
        else if (http_compressed_method == "br")
            return std::make_shared<BrotliReadBuffer>(*raw_buffer);
#endif
        else
            throw Exception("Unknown Content-Encoding of HTTP request: " + http_compressed_method, ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    return raw_buffer;
}

ReadBufferPtr HTTPInputStreams::createInternalCompressedBuffer(HTMLForm & params, ReadBufferPtr & http_maybe_encoding_buffer) const
{
    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    if (params.getParsed<bool>("decompress", false))
        return std::make_unique<CompressedReadBuffer>(*http_maybe_encoding_buffer);

    return http_maybe_encoding_buffer;
}

}
