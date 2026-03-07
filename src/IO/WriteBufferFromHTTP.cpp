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

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const HTTPConnectionGroupType & connection_group_,
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type_,
    const std::string & content_encoding_,
    const HTTPHeaderEntries & additional_headers_,
    const ConnectionTimeouts & timeouts_,
    size_t buffer_size_,
    ProxyConfiguration proxy_configuration,
    size_t max_redirects_
)
    : WriteBufferFromOStream(buffer_size_)
    , connection_group(connection_group_)
    , initial_uri(uri)
    , content_type(content_type_)
    , content_encoding(content_encoding_)
    , additional_headers(additional_headers_)
    , timeouts(timeouts_)
    , buffer_size(buffer_size_)
    , max_redirects(max_redirects_)
    , session{makeHTTPSession(connection_group_, uri, timeouts_, proxy_configuration)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    if (uri.getPort())
        request.setHost(uri.getHost(), uri.getPort());
    else
        request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(true);

    if (!content_encoding_.empty())
        request.set("Content-Encoding", content_encoding_);

    for (const auto & header: additional_headers_)
        request.add(header.name, header.value);

    if (!content_type_.empty() && !request.has("Content-Type"))
    {
        request.set("Content-Type", content_type_);
    }

    LOG_TRACE((getLogger("WriteBufferToHTTP")), "Sending request to {}", uri.toString());

    ProfileEvents::increment(ProfileEvents::WriteBufferFromHTTPRequestsSent);
    ostr = &session->sendRequest(request);
}

void WriteBufferFromHTTP::nextImpl()
{
    ProfileEvents::increment(ProfileEvents::WriteBufferFromHTTPBytes, offset());
    WriteBufferFromOStream::nextImpl();
}

void WriteBufferFromHTTP::finalizeImpl()
{
    // Make sure the content in the buffer has been flushed
    this->next();

    /// When max_redirects > 0, accept HTTP 3xx redirect responses as success.
    /// The request body has already been fully sent to the original server via
    /// the chunked transfer stream. Since we cannot re-send the body to the
    /// redirect target (it was streamed, not buffered), we treat the redirect
    /// as an acknowledgment that the server received the data.
    /// This covers the common case of servers/proxies that accept POST data
    /// and respond with a redirect to a result or canonical URL.
    bool allow_redirects = max_redirects > 0;
    receiveResponse(*session, request, response, allow_redirects);

    if (isRedirect(response.getStatus()) && allow_redirects)
    {
        auto location = response.get("Location", "");
        LOG_INFO(
            getLogger("WriteBufferToHTTP"),
            "POST/PUT to {} returned redirect (HTTP {}) to '{}'. "
            "Data was already sent to the original URL; treating redirect as success.",
            initial_uri.toString(),
            static_cast<int>(response.getStatus()),
            location);
    }

    WriteBufferFromOStream::finalizeImpl();
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
        connection_group, uri, method, content_type, content_encoding, additional_headers, timeouts, buffer_size_, proxy_configuration, max_redirects_));

    return ptr;
}

}
