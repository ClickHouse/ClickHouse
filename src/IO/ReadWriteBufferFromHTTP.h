#pragma once

#include <functional>
#include <base/types.h>
#include <base/sleep.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadSettings.h>
#include <Poco/Any.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/RemoteHostFilter.h>
#include <base/logger_useful.h>
#include <Poco/URIStreamFactory.h>

#include <Common/config.h>


namespace DB
{
/** Perform HTTP POST request and provide response to read.
  */

namespace ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
}

template <typename SessionPtr>
class UpdatableSessionBase
{
protected:
    SessionPtr session;
    UInt64 redirects { 0 };
    Poco::URI initial_uri;
    const ConnectionTimeouts & timeouts;
    UInt64 max_redirects;

public:
    virtual void buildNewSession(const Poco::URI & uri) = 0;

    explicit UpdatableSessionBase(const Poco::URI uri,
        const ConnectionTimeouts & timeouts_,
        UInt64 max_redirects_)
        : initial_uri { uri }
        , timeouts { timeouts_ }
        , max_redirects { max_redirects_ }
    {
    }

    SessionPtr getSession()
    {
        return session;
    }

    void updateSession(const Poco::URI & uri)
    {
        ++redirects;
        if (redirects <= max_redirects)
        {
            buildNewSession(uri);
        }
        else
        {
            throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", initial_uri.toString());
        }
    }

    virtual ~UpdatableSessionBase() = default;
};


namespace detail
{
    template <typename UpdatableSessionPtr>
    class ReadWriteBufferFromHTTPBase : public ReadBuffer
    {
    public:
        using HTTPHeaderEntry = std::tuple<std::string, std::string>;
        using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>;

    protected:
        Poco::URI uri;
        std::string method;
        std::string content_encoding;

        UpdatableSessionPtr session;
        std::istream * istr; /// owned by session
        std::unique_ptr<ReadBuffer> impl;
        std::function<void(std::ostream &)> out_stream_callback;
        const Poco::Net::HTTPBasicCredentials & credentials;
        std::vector<Poco::Net::HTTPCookie> cookies;
        HTTPHeaderEntries http_header_entries;
        RemoteHostFilter remote_host_filter;
        std::function<void(size_t)> next_callback;

        size_t buffer_size;
        ReadSettings settings;

        std::istream * call(Poco::URI uri_, Poco::Net::HTTPResponse & response)
        {
            // With empty path poco will send "POST  HTTP/1.1" its bug.
            if (uri_.getPath().empty())
                uri_.setPath("/");

            Poco::Net::HTTPRequest request(method, uri_.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            request.setHost(uri_.getHost()); // use original, not resolved host name in header

            if (out_stream_callback)
                request.setChunkedTransferEncoding(true);

            for (auto & http_header_entry: http_header_entries)
            {
                request.set(std::get<0>(http_header_entry), std::get<1>(http_header_entry));
            }

            if (!credentials.getUsername().empty())
                credentials.authenticate(request);

            LOG_TRACE((&Poco::Logger::get("ReadWriteBufferFromHTTP")), "Sending request to {}", uri_.toString());

            auto sess = session->getSession();

            try
            {
                auto & stream_out = sess->sendRequest(request);

                if (out_stream_callback)
                    out_stream_callback(stream_out);

                istr = receiveResponse(*sess, request, response, true);
                response.getCookies(cookies);

                content_encoding = response.get("Content-Encoding", "");
                return istr;

            }
            catch (const Poco::Exception & e)
            {
                /// We use session data storage as storage for exception text
                /// Depend on it we can deduce to reconnect session or reresolve session host
                sess->attachSessionData(e.message());
                throw;
            }
        }

    private:
        bool use_external_buffer;

    public:
        using NextCallback = std::function<void(size_t)>;
        using OutStreamCallback = std::function<void(std::ostream &)>;

        explicit ReadWriteBufferFromHTTPBase(
            UpdatableSessionPtr session_,
            Poco::URI uri_,
            const std::string & method_ = {},
            OutStreamCallback out_stream_callback_ = {},
            const Poco::Net::HTTPBasicCredentials & credentials_ = {},
            size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
            const ReadSettings & settings_ = {},
            HTTPHeaderEntries http_header_entries_ = {},
            const RemoteHostFilter & remote_host_filter_ = {},
            bool use_external_buffer_ = false)
            : ReadBuffer(nullptr, 0)
            , uri {uri_}
            , method {!method_.empty() ? method_ : out_stream_callback_ ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET}
            , session {session_}
            , out_stream_callback {out_stream_callback_}
            , credentials {credentials_}
            , http_header_entries {http_header_entries_}
            , remote_host_filter {remote_host_filter_}
            , buffer_size {buffer_size_}
            , settings {settings_}
            , use_external_buffer {use_external_buffer_}
        {
            initialize();
        }

        void initialize()
        {

            Poco::Net::HTTPResponse response;
            istr = call(uri, response);

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect(response.get("Location"));
                remote_host_filter.checkURL(uri_redirect);

                session->updateSession(uri_redirect);

                istr = call(uri_redirect, response);
            }

            try
            {
                impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size);

                if (use_external_buffer)
                {
                    /**
                    * See comment 30 lines below.
                    */
                    impl->set(internal_buffer.begin(), internal_buffer.size());
                    assert(working_buffer.begin() != nullptr);
                    assert(!internal_buffer.empty());
                }
            }
            catch (const Poco::Exception & e)
            {
                /// We use session data storage as storage for exception text
                /// Depend on it we can deduce to reconnect session or reresolve session host
                auto sess = session->getSession();
                sess->attachSessionData(e.message());
                throw;
            }
        }

        bool nextImpl() override
        {
            if (next_callback)
                next_callback(count());

            if (use_external_buffer)
            {
                /**
                * use_external_buffer -- means we read into the buffer which
                * was passed to us from somewhere else. We do not check whether
                * previously returned buffer was read or not (no hasPendingData() check is needed),
                * because this branch means we are prefetching data,
                * each nextImpl() call we can fill a different buffer.
                */
                impl->set(internal_buffer.begin(), internal_buffer.size());
                assert(working_buffer.begin() != nullptr);
                assert(!internal_buffer.empty());
            }
            else
            {
                /**
                * impl was initialized before, pass position() to it to make
                * sure there is no pending data which was not read.
                */
                if (!working_buffer.empty())
                    impl->position() = position();
            }

            if (!impl->next())
                return false;

            internal_buffer = impl->buffer();
            working_buffer = internal_buffer;
            return true;
        }

        std::string getResponseCookie(const std::string & name, const std::string & def) const
        {
            for (const auto & cookie : cookies)
                if (cookie.getName() == name)
                    return cookie.getValue();
            return def;
        }

        /// Set function to call on each nextImpl, useful when you need to track
        /// progress.
        /// NOTE: parameter on each call is not incremental -- it's all bytes count
        /// passed through the buffer
        void setNextCallback(NextCallback next_callback_)
        {
            next_callback = next_callback_;
            /// Some data maybe already read
            next_callback(count());
        }

        const std::string & getCompressionMethod() const
        {
            return content_encoding;
        }
    };
}

class UpdatableSession : public UpdatableSessionBase<HTTPSessionPtr>
{
    using Parent = UpdatableSessionBase<HTTPSessionPtr>;

public:
    explicit UpdatableSession(
        const Poco::URI uri,
        const ConnectionTimeouts & timeouts_,
        const UInt64 max_redirects_)
        : Parent(uri, timeouts_, max_redirects_)
    {
        session = makeHTTPSession(initial_uri, timeouts);
    }

    void buildNewSession(const Poco::URI & uri) override
    {
        session = makeHTTPSession(uri, timeouts);
    }
};

class ReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession>>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession>>;

public:
    explicit ReadWriteBufferFromHTTP(
        Poco::URI uri_,
        const std::string & method_,
        OutStreamCallback out_stream_callback_,
        const ConnectionTimeouts & timeouts,
        const UInt64 max_redirects = 0,
        const Poco::Net::HTTPBasicCredentials & credentials_ = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const ReadSettings & settings_ = {},
        const HTTPHeaderEntries & http_header_entries_ = {},
        const RemoteHostFilter & remote_host_filter_ = {},
        bool use_external_buffer_ = false)
        : Parent(std::make_shared<UpdatableSession>(uri_, timeouts, max_redirects),
                 uri_, method_, out_stream_callback_, credentials_, buffer_size_,
                 settings_, http_header_entries_, remote_host_filter_, use_external_buffer_)
    {
    }
};

class UpdatablePooledSession : public UpdatableSessionBase<PooledHTTPSessionPtr>
{
    using Parent = UpdatableSessionBase<PooledHTTPSessionPtr>;

private:
    size_t per_endpoint_pool_size;

public:
    explicit UpdatablePooledSession(const Poco::URI uri,
        const ConnectionTimeouts & timeouts_,
        const UInt64 max_redirects_,
        size_t per_endpoint_pool_size_)
        : Parent(uri, timeouts_, max_redirects_)
        , per_endpoint_pool_size { per_endpoint_pool_size_ }
    {
        session = makePooledHTTPSession(initial_uri, timeouts, per_endpoint_pool_size);
    }

    void buildNewSession(const Poco::URI & uri) override
    {
       session = makePooledHTTPSession(uri, timeouts, per_endpoint_pool_size);
    }
};

class PooledReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatablePooledSession>>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatablePooledSession>>;

public:
    explicit PooledReadWriteBufferFromHTTP(Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback_ = {},
        const ConnectionTimeouts & timeouts_ = {},
        const Poco::Net::HTTPBasicCredentials & credentials_ = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const UInt64 max_redirects = 0,
        size_t max_connections_per_endpoint = DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT)
        : Parent(std::make_shared<UpdatablePooledSession>(uri_, timeouts_, max_redirects, max_connections_per_endpoint),
              uri_,
              method_,
              out_stream_callback_,
              credentials_,
              buffer_size_)
    {
    }
};

}
