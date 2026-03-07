#include <IO/WriteBufferFromHTTP.h>

#include <filesystem>
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
    extern const int TOO_MANY_REDIRECTS;
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

    bool allow_redirects = max_redirects > 0;
    receiveResponse(*session, request, response, allow_redirects);

    size_t redirects = 0;
    while (isRedirect(response.getStatus()) && allow_redirects)
    {
        ++redirects;
        if (redirects > max_redirects)
            throw Exception(
                ErrorCodes::TOO_MANY_REDIRECTS,
                "Too many redirects while trying to write to {}."
                " You can {} redirects by changing the setting 'max_http_post_redirects'."
                " Example: `SET max_http_post_redirects = 10`."
                " Redirects are restricted to prevent possible attack when a malicious server"
                " redirects to an internal resource, bypassing the authentication or firewall.",
                initial_uri.toString(), max_redirects ? "increase the allowed maximum number of" : "allow");

        auto location = response.get("Location");
        auto redirect_uri = Poco::URI(location);
        if (redirect_uri.isRelative())
        {
            auto path = std::filesystem::weakly_canonical(
                std::filesystem::path(initial_uri.getPath()) / location);
            redirect_uri = initial_uri;
            redirect_uri.setPath(path);
        }

        LOG_TRACE((getLogger("WriteBufferToHTTP")), "Redirecting POST/PUT to {}", redirect_uri.toString());

        /// Open new session to the redirect target and re-send an empty request
        /// (the data has already been sent; this handles the redirect for the response)
        session = makeHTTPSession(connection_group, redirect_uri, timeouts);
        request.setURI(redirect_uri.getPathAndQuery());
        if (redirect_uri.getPort())
            request.setHost(redirect_uri.getHost(), redirect_uri.getPort());
        else
            request.setHost(redirect_uri.getHost());
        request.setChunkedTransferEncoding(false);
        request.setContentLength(0);

        ProfileEvents::increment(ProfileEvents::WriteBufferFromHTTPRequestsSent);
        session->sendRequest(request);
        receiveResponse(*session, request, response, true);
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
