#pragma once

#include <IO/WriteBufferFromHTTP.h>

namespace DB
{

class WriteBufferFromHTTPBuilder
{
public:
    WriteBufferFromHTTPBuilder & withURI(const Poco::URI & uri_);
    WriteBufferFromHTTPBuilder & withMethod(const std::string & method_);
    WriteBufferFromHTTPBuilder & withContentType(const std::string & content_type_);
    WriteBufferFromHTTPBuilder & withContentEncoding(const std::string & content_encoding_);
    WriteBufferFromHTTPBuilder & withAdditionalHeaders(const HTTPHeaderEntries & additional_headers_);
    WriteBufferFromHTTPBuilder & withTimeouts(const ConnectionTimeouts & timeouts_);
    WriteBufferFromHTTPBuilder & withBufferSize(size_t buffer_size_);
    WriteBufferFromHTTPBuilder & withProxyConfiguration(Poco::Net::HTTPClientSession::ProxyConfig proxy_configuration_);

    std::unique_ptr<WriteBufferFromHTTP> build();

private:
    Poco::URI uri;
    std::string method = Poco::Net::HTTPRequest::HTTP_POST;
    std::string content_type;
    std::string content_encoding;
    HTTPHeaderEntries additional_headers;
    ConnectionTimeouts timeouts;
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    Poco::Net::HTTPClientSession::ProxyConfig proxy_configuration;
};

}

