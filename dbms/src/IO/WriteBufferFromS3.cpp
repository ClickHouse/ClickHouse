#include <IO/WriteBufferFromS3.h>

#include <common/logger_useful.h>


#define DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT 2

namespace DB
{

WriteBufferFromS3::WriteBufferFromS3(
    const Poco::URI & uri_, const ConnectionTimeouts & timeouts_, 
    const Poco::Net::HTTPBasicCredentials & credentials, size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , uri {uri_}
    , timeouts {timeouts_}
    , auth_request {Poco::Net::HTTPRequest::HTTP_PUT, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    ostr = &temporary_stream;
    if (!credentials.getUsername().empty())
        credentials.authenticate(auth_request);
}

void WriteBufferFromS3::finalize()
{
    const std::string & data = temporary_stream.str();

    Poco::Net::HTTPResponse response;
    std::unique_ptr<Poco::Net::HTTPRequest> request;
    HTTPSessionPtr session;

    std::istream * istr; /// owned by session
    for (int i = 0; i < DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT; ++i)
    {
        session = makeHTTPSession(uri, timeouts);
        request = std::make_unique<Poco::Net::HTTPRequest>(Poco::Net::HTTPRequest::HTTP_PUT, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
        request->setHost(uri.getHost()); // use original, not resolved host name in header

        if (auth_request.hasCredentials()) {
            Poco::Net::HTTPBasicCredentials credentials(auth_request);
            credentials.authenticate(*request);
        }

        // request.setChunkedTransferEncoding(true);
        // Chunked transfers require additional logic, see:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html

        request->setExpectContinue(true);

        request->setContentLength(data.size());

        LOG_TRACE((&Logger::get("WriteBufferFromS3")), "Sending request to " << uri.toString());

        ostr = &session->sendRequest(*request);
//        if (session->peekResponse(response))
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
    }
    assertResponseIsOk(*request, response, istr);
}

}
