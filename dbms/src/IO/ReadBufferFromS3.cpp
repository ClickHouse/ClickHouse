#include <IO/ReadBufferFromS3.h>

#include <IO/ReadBufferFromIStream.h>

#include <common/logger_useful.h>


namespace DB
{

const int DEFAULT_S3_MAX_FOLLOW_GET_REDIRECT = 2;

ReadBufferFromS3::ReadBufferFromS3(Poco::URI uri_,
    const ConnectionTimeouts & timeouts,
    const Poco::Net::HTTPBasicCredentials & credentials,
    size_t buffer_size_)
    : ReadBuffer(nullptr, 0)
    , uri {uri_}
    , method {Poco::Net::HTTPRequest::HTTP_GET}
    , session {makeHTTPSession(uri_, timeouts)}
{
    Poco::Net::HTTPResponse response;
    std::unique_ptr<Poco::Net::HTTPRequest> request;

    for (int i = 0; i < DEFAULT_S3_MAX_FOLLOW_GET_REDIRECT; ++i)
    {
        // With empty path poco will send "POST  HTTP/1.1" its bug.
        if (uri.getPath().empty())
            uri.setPath("/");

        request = std::make_unique<Poco::Net::HTTPRequest>(method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
        request->setHost(uri.getHost()); // use original, not resolved host name in header

        if (!credentials.getUsername().empty())
            credentials.authenticate(*request);

        LOG_TRACE((&Logger::get("ReadBufferFromS3")), "Sending request to " << uri.toString());

        session->sendRequest(*request);

        istr = &session->receiveResponse(response);

        // Handle 307 Temporary Redirect in order to allow request redirection
        // See https://docs.aws.amazon.com/AmazonS3/latest/dev/Redirects.html
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT)
            break;

        auto location_iterator = response.find("Location");
        if (location_iterator == response.end())
            break;

        uri = location_iterator->second;
        session = makeHTTPSession(uri, timeouts);
    }

    assertResponseIsOk(*request, response, *istr);
    impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
}


bool ReadBufferFromS3::nextImpl()
{
    if (!impl->next())
        return false;
    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    return true;
}

}
