#include <IO/WriteBufferFromHTTP.h>

#include <Common/logger_useful.h>
#include <Common/ProxyConfigurationResolverProvider.h>
#include <Interpreters/Context.h>


namespace ProfileEvents
{
    extern const Event WriteBufferFromHTTPRequestsSent;
    extern const Event WriteBufferFromHTTPBytes;
}

namespace DB
{

namespace detail
{

WriteBufferFromHTTPRequestSender::WriteBufferFromHTTPRequestSender(
    const HTTPConnectionGroupType & connection_group,
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers,
    const ConnectionTimeouts & timeouts,
    const ProxyConfiguration & proxy_configuration)
    : http_session{makeHTTPSession(connection_group, uri, timeouts, proxy_configuration)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    if (uri.getPort())
        request.setHost(uri.getHost(), uri.getPort());
    else
        request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(true);

    if (!content_encoding.empty())
        request.set("Content-Encoding", content_encoding);

    for (const auto & header : additional_headers)
        request.add(header.name, header.value);

    if (!content_type.empty() && !request.has("Content-Type"))
    {
        request.set("Content-Type", content_type);
    }

    LOG_TRACE((getLogger("WriteBufferToHTTP")), "Sending request to {}", uri.toString());

    ProfileEvents::increment(ProfileEvents::WriteBufferFromHTTPRequestsSent);
    body_info = http_session->sendRequestHeaders(request);
}

}

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const HTTPConnectionGroupType & connection_group,
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_,
    ProxyConfiguration proxy_configuration
)
    : detail::WriteBufferFromHTTPRequestSender(
          connection_group, uri, method, content_type, content_encoding, additional_headers, timeouts, proxy_configuration)
    , HTTPRequestBodyWriteBuffer(*http_session, body_info, buffer_size_)
{
}

void WriteBufferFromHTTP::nextImpl()
{
    ProfileEvents::increment(ProfileEvents::WriteBufferFromHTTPBytes, offset());
    HTTPRequestBodyWriteBuffer::nextImpl();
}

void WriteBufferFromHTTP::finalizeImpl()
{
    /// Sends the rest of the body and the terminating chunk.
    HTTPRequestBodyWriteBuffer::finalizeImpl();

    receiveResponse(*http_session, request, response, false);
    /// TODO: Response body is ignored.
}

std::unique_ptr<WriteBufferFromHTTP> BuilderWriteBufferFromHTTP::create()
{
    ProxyConfiguration proxy_configuration;

    if (!bypass_proxy)
    {
        auto proxy_protocol = ProxyConfiguration::protocolFromString(uri.getScheme());
        proxy_configuration = ProxyConfigurationResolverProvider::get(proxy_protocol)->resolve();
    }

    /// WriteBufferFromHTTP constructor is private and can't be used in `make_unique`
    std::unique_ptr<WriteBufferFromHTTP> ptr(new WriteBufferFromHTTP(
        connection_group, uri, method, content_type, content_encoding, additional_headers, timeouts, buffer_size_, proxy_configuration));

    return ptr;
}

}
