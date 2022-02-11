#include <IO/WriteBufferFromHTTP.h>

#include <base/logger_useful.h>


namespace DB
{

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const CompressionMethod compression_method,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , session{makeHTTPSession(uri, timeouts)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(true);

    if (!content_type.empty())
    {
        request.set("Content-Type", content_type);
    }

    std::string encoding = toContentEncodingName(compression_method);
    if (!encoding.empty())
        request.set("Content-Encoding", encoding);

    LOG_TRACE((&Poco::Logger::get("WriteBufferToHTTP")), "Sending request to {}", uri.toString());

    ostr = &session->sendRequest(request);
}

void WriteBufferFromHTTP::finalizeImpl()
{
    // for compressed body, the data is stored in buffered first
    // here, make sure the content in the buffer has been flushed
    this->nextImpl();

    receiveResponse(*session, request, response, false);
    /// TODO: Response body is ignored.
}

}
