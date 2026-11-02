#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBuffer.h>
#include <IO/HTTP/HTTPClientIO.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>


namespace DB
{

namespace detail
{

/// Opens the session and sends the request headers, so that the framing of the body is known
/// before the buffer of `WriteBufferFromHTTP` is constructed. It is a base class rather than a
/// member because base classes are initialized first.
struct WriteBufferFromHTTPRequestSender
{
    WriteBufferFromHTTPRequestSender(
        const HTTPConnectionGroupType & connection_group,
        const Poco::URI & uri,
        const std::string & method,
        const std::string & content_type,
        const std::string & content_encoding,
        const HTTPHeaderEntries & additional_headers,
        const ConnectionTimeouts & timeouts,
        const ProxyConfiguration & proxy_configuration);

    /// Named to avoid clashing with the session of `HTTPRequestBodyWriteBuffer`.
    HTTPSessionPtr http_session;
    Poco::Net::HTTPRequest request;
    Poco::Net::HTTPResponse response;
    Poco::Net::HTTPClientSession::BodyInfo body_info;
};

}

/* Perform HTTP POST/PUT request.
 */
class WriteBufferFromHTTP : private detail::WriteBufferFromHTTPRequestSender, public HTTPRequestBodyWriteBuffer
{
    friend class BuilderWriteBufferFromHTTP;

    explicit WriteBufferFromHTTP(const HTTPConnectionGroupType & connection_group,
                                 const Poco::URI & uri,
                                 const std::string & method = Poco::Net::HTTPRequest::HTTP_POST, // POST or PUT only
                                 const std::string & content_type = "",
                                 const std::string & content_encoding = "",
                                 const HTTPHeaderEntries & additional_headers = {},
                                 const ConnectionTimeouts & timeouts = {},
                                 size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
                                 ProxyConfiguration proxy_configuration = {});

    // Counts the counter WriteBufferFromHTTPBytes
    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalizeImpl() override;
};

class BuilderWriteBufferFromHTTP
{
private:
    Poco::URI uri;
    HTTPConnectionGroupType connection_group{};
    std::string method = Poco::Net::HTTPRequest::HTTP_POST; // POST or PUT only
    bool bypass_proxy = false;
    std::string content_type;
    std::string content_encoding;
    HTTPHeaderEntries additional_headers = {};
    ConnectionTimeouts timeouts = {};
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE;

public:
    explicit BuilderWriteBufferFromHTTP(const Poco::URI & uri_) : uri(uri_)
    {
    }

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define setterMember(name, member) \
    BuilderWriteBufferFromHTTP & name(decltype(BuilderWriteBufferFromHTTP::member) arg_##member) \
    { \
        member = std::move(arg_##member); \
        return *this; \
    }

    setterMember(withConnectionGroup, connection_group)
    setterMember(withMethod, method)
    setterMember(withBypassProxy, bypass_proxy)
    setterMember(withContentType, content_type)
    setterMember(withContentEncoding, content_encoding)
    setterMember(withAdditionalHeaders, additional_headers)
    setterMember(withTimeouts, timeouts)
    setterMember(withBufferSize, buffer_size_)
#undef setterMember
/// NOLINTEND(bugprone-macro-parentheses)

    std::unique_ptr<WriteBufferFromHTTP> create();
};

}
