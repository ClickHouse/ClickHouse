#include <IO/WriteBufferFromHTTPBuilder.h>

namespace DB
{

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withURI(const Poco::URI & uri_)
{
    uri = uri_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withMethod(const std::string & method_)
{
    method = method_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withContentType(const std::string & content_type_)
{
    content_type = content_type_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withContentEncoding(const std::string & content_encoding_)
{
    content_encoding = content_encoding_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withAdditionalHeaders(const HTTPHeaderEntries & additional_headers_)
{
    additional_headers = additional_headers_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withTimeouts(const ConnectionTimeouts & timeouts_)
{
    timeouts = timeouts_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withBufferSize(size_t buffer_size_)
{
    buffer_size = buffer_size_;
    return *this;
}

WriteBufferFromHTTPBuilder & WriteBufferFromHTTPBuilder::withProxyConfiguration(
    Poco::Net::HTTPClientSession::ProxyConfig proxy_configuration_
)
{
    proxy_configuration = proxy_configuration_;
    return *this;
}

std::unique_ptr<WriteBufferFromHTTP> WriteBufferFromHTTPBuilder::build()
{
    return std::make_unique<WriteBufferFromHTTP>(
        uri,
        method,
        content_type,
        content_encoding,
        additional_headers,
        timeouts,
        buffer_size,
        proxy_configuration
    );
}

}
