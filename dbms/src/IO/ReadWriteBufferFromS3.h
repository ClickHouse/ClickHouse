#pragma once

#include <functional>
#include <memory>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromOStream.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/config.h>
#include <common/logger_useful.h>


#define DEFAULT_S3_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_S3_READ_BUFFER_CONNECTION_TIMEOUT 1
#define DEFAULT_S3_MAX_FOLLOW_REDIRECT 2

namespace DB
{
/** Perform S3 HTTP POST request and provide response to read.
  */

namespace detail
{
    template <typename SessionPtr>//FIXME Можно избавиться от template, или переделать на нормальное.
    class ReadWriteBufferFromS3Base : public ReadBuffer
    {
    protected:
        Poco::URI uri;
        std::string method;

        SessionPtr session;
        std::istream * istr; /// owned by session
        std::unique_ptr<ReadBuffer> impl;

    public:
        using OutStreamCallback = std::function<void(std::ostream &)>;

        explicit ReadWriteBufferFromS3Base(Poco::URI uri,
            const ConnectionTimeouts & timeouts = {},
            const std::string & method = {},
            OutStreamCallback out_stream_callback = {},
            const Poco::Net::HTTPBasicCredentials & credentials = {},
            size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
            : ReadBuffer(nullptr, 0)
            , uri {uri}
            , method {!method.empty() ? method : out_stream_callback ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET}
            , session(makeHTTPSession(uri, timeouts))
        {
            Poco::Net::HTTPResponse response;
            std::unique_ptr<Poco::Net::HTTPRequest> request;

            for (int i = 0; i < DEFAULT_S3_MAX_FOLLOW_REDIRECT; ++i)
            {
                // With empty path poco will send "POST  HTTP/1.1" its bug.
                if (uri.getPath().empty())
                    uri.setPath("/");

                request = std::make_unique<Poco::Net::HTTPRequest>(method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
                request->setHost(uri.getHost()); // use original, not resolved host name in header

                if (out_stream_callback)
                    request->setChunkedTransferEncoding(true);

                if (!credentials.getUsername().empty())
                    credentials.authenticate(*request);

                LOG_TRACE((&Logger::get("ReadWriteBufferFromS3")), "Sending request to " << uri.toString());

                auto & stream_out = session->sendRequest(*request);

                if (out_stream_callback)
                    out_stream_callback(stream_out);

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
            impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
        }


        bool nextImpl() override
        {
            if (!impl->next())
                return false;
            internal_buffer = impl->buffer();
            working_buffer = internal_buffer;
            return true;
        }
    };
}

class ReadWriteBufferFromS3 : public detail::ReadWriteBufferFromS3Base<HTTPSessionPtr>
{
    using Parent = detail::ReadWriteBufferFromS3Base<HTTPSessionPtr>;

public:
    explicit ReadWriteBufferFromS3(Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
        : Parent(uri_, timeouts, method_, out_stream_callback, credentials, buffer_size_)
    {
    }
};

/* Perform S3 HTTP POST/PUT request.
 */
class WriteBufferFromS3 : public WriteBufferFromOStream
{
private:
    HTTPSessionPtr session;
    Poco::Net::HTTPRequest request;
    Poco::Net::HTTPResponse response;

public:
    explicit WriteBufferFromS3(const Poco::URI & uri,
        const std::string & method = Poco::Net::HTTPRequest::HTTP_POST, // POST for inserting, PUT for replacing.
        const ConnectionTimeouts & timeouts = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    /// Receives response from the server after sending all data.
    void finalize();
};

}
