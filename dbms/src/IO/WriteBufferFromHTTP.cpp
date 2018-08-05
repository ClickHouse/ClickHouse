#include <IO/WriteBufferFromHTTP.h>

#include <IO/HTTPCommon.h>
#include <common/logger_useful.h>


namespace DB
{

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const Poco::URI & uri, const std::string & method, const ConnectionTimeouts & timeouts, size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , session{makeHTTPSession(uri, timeouts)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    request.setHost(uri.getHost());
    request.setChunkedTransferEncoding(true);

    LOG_TRACE((&Logger::get("WriteBufferToHTTP")), "Sending request to " << uri.toString());

    ostr = &session->sendRequest(request);
}

void WriteBufferFromHTTP::finalize()
{
    receiveResponse(*session, request, response);
    /// TODO: Response body is ignored.
}

}
