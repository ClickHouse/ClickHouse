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

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

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
    bool allow_redirects_
)
    : WriteBufferFromOStream(buffer_size_)
    , initial_uri(uri)
    , allow_redirects(allow_redirects_)
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

    /// When allow_redirects is true, accept HTTP 301/302/303 redirect responses as success.
    /// The request body has already been fully sent to the original server via
    /// the chunked transfer stream. Since we cannot re-send the body to the
    /// redirect target (it was streamed, not buffered), we treat the redirect
    /// as an acknowledgment that the server received the data.
    /// This covers the common case of servers/proxies that accept POST data
    /// and respond with a redirect to a result or canonical URL.
    /// The method-preserving statuses 307/308 are rejected even with allow_redirects,
    /// because they explicitly require the client to replay the request body.
    receiveResponse(*session, request, response, allow_redirects);

    const auto status = response.getStatus();
    if (allow_redirects && isRedirect(status))
    {
        /// Method-preserving redirects (307/308) explicitly ask the client to
        /// retry the same request at a new URL. Because the body was streamed
        /// to the original server and cannot be replayed, treating these as
        /// success would silently lose data. Reject them so the caller sees a
        /// real error instead of a false acknowledgment.
        if (status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT
            || status == Poco::Net::HTTPResponse::HTTP_PERMANENT_REDIRECT)
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "POST/PUT to {} returned HTTP {} ({}). This is a method-preserving redirect: "
                "the server is asking to repeat the request at the URL in the Location header, "
                "but the request body was already streamed and cannot be replayed, so the write "
                "may not have been committed. Configure the endpoint to respond with 301, 302, or 303 "
                "to acknowledge the write, or point the client at the final URL directly.",
                initial_uri.getHost(),
                static_cast<int>(status),
                response.getReason());
        }

        /// A genuine redirect acknowledgment carries a Location header pointing at the
        /// canonical/result URL. A bare 301/302/303 without a Location header is not a
        /// real redirect (for example, a proxy, auth, or error page that happens to use
        /// a 3xx status); treating it as success would silently report a committed write
        /// that may never have happened. Require a non-empty Location before accepting.
        if (response.get("Location", "").empty())
        {
            throw Exception(
                ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
                "POST/PUT to {} returned HTTP {} ({}) without a Location header. "
                "A redirect status is only accepted as a write acknowledgment when it carries "
                "a Location header pointing at the canonical or result URL.",
                initial_uri.getHost(),
                static_cast<int>(status),
                response.getReason());
        }

        LOG_TRACE(
            getLogger("WriteBufferToHTTP"),
            "POST/PUT to {} returned redirect (HTTP {}); "
            "data was already sent to the original URL, treating redirect as success.",
            initial_uri.getHost(),
            static_cast<int>(status));
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
        connection_group, uri, method, content_type, content_encoding, additional_headers, timeouts, buffer_size_, proxy_configuration, allow_redirects_));

    return ptr;
}

}
