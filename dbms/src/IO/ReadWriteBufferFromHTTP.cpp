#include <IO/ReadWriteBufferFromHTTP.h>

#include <Common/config.h>
#include <Core/Types.h>
#include <IO/ReadBufferFromIStream.h>
#include <Common/DNSResolver.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Version.h>
#include <common/logger_useful.h>

#if USE_POCO_NETSSL
#include <Poco/Net/HTTPSClientSession.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
}


ReadWriteBufferFromHTTP::ReadWriteBufferFromHTTP(const Poco::URI & uri,
    const std::string & method_,
    OutStreamCallback out_stream_callback,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_)
    : ReadBuffer(nullptr, 0),
      uri{uri},
      method{!method_.empty() ? method_ : out_stream_callback ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET},
      timeouts{timeouts},
      is_ssl{uri.getScheme() == "https"},
      session
{
    std::unique_ptr<Poco::Net::HTTPClientSession>(
#if USE_POCO_NETSSL
        is_ssl ? new Poco::Net::HTTPSClientSession :
#endif
               new Poco::Net::HTTPClientSession)
}
{
    session->setHost(DNSResolver::instance().resolveHost(uri.getHost()).toString());
    session->setPort(uri.getPort());

#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000
    session->setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
#else
    session->setTimeout(timeouts.connection_timeout);
#endif

    Poco::Net::HTTPRequest request(method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.setHost(uri.getHost()); // use original, not resolved host name in header

    if (out_stream_callback)
        request.setChunkedTransferEncoding(true);

    Poco::Net::HTTPResponse response;

    LOG_TRACE((&Logger::get("ReadWriteBufferFromHTTP")), "Sending request to " << uri.toString());

    auto & stream_out = session->sendRequest(request);

    if (out_stream_callback)
        out_stream_callback(stream_out);

    istr = &session->receiveResponse(response);

    auto status = response.getStatus();

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::stringstream error_message;
        error_message << "Received error from remote server " << uri.toString() << ". HTTP status code: " << status << " "
                      << response.getReason() << ", body: " << istr->rdbuf();

        throw Exception(error_message.str(),
            status == HTTP_TOO_MANY_REQUESTS ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                                             : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    }

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
