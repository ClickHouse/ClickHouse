#include <IO/WriteBufferFromS3.h>

#include <common/logger_useful.h>


#define DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT 2

namespace DB
{

WriteBufferFromS3::WriteBufferFromS3(
    const Poco::URI & uri_, const ConnectionTimeouts & timeouts_, 
    const Poco::Net::HTTPBasicCredentials & credentials_, size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , uri {uri_}
    , timeouts {timeouts_}
    , credentials {credentials_}
    , session {makeHTTPSession(uri_, timeouts_)}
{
    ostr = &temporary_stream;
}

void WriteBufferFromS3::finalize()
{
    const std::string & data = temporary_stream.str();

    std::unique_ptr<Poco::Net::HTTPRequest> request;
    for (int i = 0; i < DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT; ++i)
    {
        request = std::make_unique<Poco::Net::HTTPRequest>(Poco::Net::HTTPRequest::HTTP_PUT, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
        request->setHost(uri.getHost()); // use original, not resolved host name in header

        if (!credentials.getUsername().empty())
            credentials.authenticate(*request);

        // request.setChunkedTransferEncoding(true);
        // Chunked transfers require additional logic, see:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html

        request->setExpectContinue(true);

        request->setContentLength(data.size());

        LOG_TRACE((&Logger::get("WriteBufferFromS3")), "Sending request to " << uri.toString());

        ostr = &session->sendRequest(*request);
        if (session->peekResponse(response))
        {
            // Received 100-continue.
            *ostr << data;
        }

        istr = &session->receiveResponse(response);

        if (response.getStatus() != 307)
            break;

        auto location_iterator = response.find("Location");
        if (location_iterator == response.end())
            break;

        uri = location_iterator->second;
        session = makeHTTPSession(uri, timeouts);
    }
    assertResponseIsOk(*request, response, istr);
}

}
