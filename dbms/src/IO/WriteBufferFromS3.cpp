#include <IO/WriteBufferFromS3.h>

#include <common/logger_useful.h>


#define DEFAULT_S3_MAX_FOLLOW_PUT_REDIRECT 2

namespace DB
{

WriteBufferFromS3::WriteBufferFromS3(
    const Poco::URI & uri_, const ConnectionTimeouts & timeouts_, 
    const Poco::Net::HTTPBasicCredentials & credentials, size_t buffer_size_)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size_, nullptr, 0)
    , uri {uri_}
    , timeouts {timeouts_}
    , auth_request {Poco::Net::HTTPRequest::HTTP_PUT, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
    , temporary_buffer {buffer_string}
{
    if (!credentials.getUsername().empty())
        credentials.authenticate(auth_request);
}


void WriteBufferFromS3::nextImpl()
{
    if (!offset())
        return;

    temporary_buffer.write(working_buffer.begin(), offset());
}


void WriteBufferFromS3::finalize()
{
    temporary_buffer.finish();

    const String & data = buffer_string;

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

        std::ostream & ostr = session->sendRequest(*request);
//        if (session->peekResponse(response)) // FIXME, shall not go next if not received 100-continue
        {
            // Received 100-continue.
            ostr << data;
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


WriteBufferFromS3::~WriteBufferFromS3()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
