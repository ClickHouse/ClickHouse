#pragma once

#include <functional>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <Poco/Any.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/config.h>
#include <common/logger_useful.h>


#define DEFAULT_HTTP_READ_BUFFER_TIMEOUT 1800
#define DEFAULT_HTTP_READ_BUFFER_CONNECTION_TIMEOUT 1

namespace DB
{
/** Perform HTTP POST request and provide response to read.
  */

namespace detail
{
    template <typename SessionPtr>
    class ReadWriteBufferFromHTTPBase : public ReadBuffer
    {
    protected:
        Poco::URI uri;
        std::string method;

        SessionPtr session;
        std::istream * istr; /// owned by session
        std::unique_ptr<ReadBuffer> impl;

    public:
        using OutStreamCallback = std::function<void(std::ostream &)>;

        explicit ReadWriteBufferFromHTTPBase(SessionPtr session_,
            Poco::URI uri_,
            const std::string & method_ = {},
            OutStreamCallback out_stream_callback = {},
            const Poco::Net::HTTPBasicCredentials & credentials = {},
            size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
            : ReadBuffer(nullptr, 0)
            , uri {uri_}
            , method {!method_.empty() ? method_ : out_stream_callback ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET}
            , session {session_}
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

            try
            {
                auto & stream_out = session->sendRequest(request);

                if (out_stream_callback)
                    out_stream_callback(stream_out);

                istr = receiveResponse(*session, request, response);

                impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size_);
            }
            catch (const Poco::Exception & e)
            {
                /// We use session data storage as storage for exception text
                /// Depend on it we can deduce to reconnect session or reresolve session host
                session->attachSessionData(e.message());
                throw;
            }
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

class ReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<HTTPSessionPtr>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<HTTPSessionPtr>;

public:
    explicit ReadWriteBufferFromHTTP(Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
        : Parent(makeHTTPSession(uri_, timeouts), uri_, method_, out_stream_callback, credentials, buffer_size_)
    {
    }
};
class PooledReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<PooledHTTPSessionPtr>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<PooledHTTPSessionPtr>;

public:
    explicit PooledReadWriteBufferFromHTTP(Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback = {},
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        size_t max_connections_per_endpoint = DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT)
        : Parent(makePooledHTTPSession(uri_, timeouts, max_connections_per_endpoint),
              uri_,
              method_,
              out_stream_callback,
              credentials,
              buffer_size_)
    {
    }
};


}
