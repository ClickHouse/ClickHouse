#include <IO/ReadWriteBufferFromHTTP.h>

#include <Common/config.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromIStream.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Version.h>
#include <common/logger_useful.h>
#include <IO/HTTPCommon.h>

namespace DB
{


ReadWriteBufferFromHTTP::ReadWriteBufferFromHTTP(Poco::URI uri,
    const std::string & method_,
    OutStreamCallback out_stream_callback,
    const ConnectionTimeouts & timeouts,
    const Poco::Net::HTTPBasicCredentials & credentials,
    size_t buffer_size_)
    : ReadBuffer(nullptr, 0),
      uri{uri},
      method{!method_.empty() ? method_ : out_stream_callback ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET},
      session{makeHTTPSession(uri, timeouts)}
{
    // With empty path poco will send "POST  HTTP/1.1" its bug.
    if (uri.getPath().empty())
        uri.setPath("/");

    Poco::Net::HTTPRequest request(method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.setHost(uri.getHost()); // use original, not resolved host name in header

    if (out_stream_callback)
        request.setChunkedTransferEncoding(true);

    if (!credentials.getUsername().empty())
        credentials.authenticate(request);

    Poco::Net::HTTPResponse response;

    LOG_TRACE((&Logger::get("ReadWriteBufferFromHTTP")), "Sending request to " << uri.toString());

    auto & stream_out = session->sendRequest(request);

    if (out_stream_callback)
        out_stream_callback(stream_out);

    istr = receiveResponse(*session, request, response);

    impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
}


bool ReadWriteBufferFromHTTP::nextImpl()
{
    if (!impl->next())
        return false;
    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    return true;
}

}
