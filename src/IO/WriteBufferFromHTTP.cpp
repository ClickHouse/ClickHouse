#include <IO/WriteBufferFromHTTP.h>

#include <Common/logger_useful.h>


namespace DB
{

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_,
    Poco::Net::HTTPClientSession::ProxyConfig proxy_configuration)
    : WriteBufferFromOStream(buffer_size_)
    , session{makeHTTPSession(uri, timeouts, proxy_configuration)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(true);

    if (!content_type.empty())
    {
        request.set("Content-Type", content_type);
    }

    if (!content_encoding.empty())
        request.set("Content-Encoding", content_encoding);

    for (const auto & header: additional_headers)
        request.add(header.name, header.value);

    LOG_TRACE((&Poco::Logger::get("WriteBufferToHTTP")), "Sending request to {}", uri.toString());

    ostr = &session->sendRequest(request);
}

void WriteBufferFromHTTP::finalizeImpl()
{
    // Make sure the content in the buffer has been flushed
    this->next();

    receiveResponse(*session, request, response, false);
    /// TODO: Response body is ignored.
}

}
