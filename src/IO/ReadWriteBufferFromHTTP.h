#pragma once

#include <functional>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WithFileName.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <base/types.h>
#include <Poco/Any.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/URIStreamFactory.h>
#include <Common/DNSResolver.h>
#include <Common/RemoteHostFilter.h>
#include "config.h"
#include "config_version.h"

#include <filesystem>

namespace DB
{

template <typename TSessionFactory>
class UpdatableSession
{
public:
    using SessionPtr = typename TSessionFactory::SessionType;

    explicit UpdatableSession(const Poco::URI & uri, UInt64 max_redirects_, std::shared_ptr<TSessionFactory> session_factory_);

    SessionPtr & getSession();

    void updateSession(const Poco::URI & uri);

    /// Thread safe.
    SessionPtr createDetachedSession(const Poco::URI & uri);

    std::shared_ptr<UpdatableSession<TSessionFactory>> clone(const Poco::URI & uri);

private:
    std::shared_ptr<TSessionFactory> session_factory;
    SessionPtr session;
    UInt64 redirects{0};
    UInt64 max_redirects;
    Poco::URI initial_uri;
};


/// Information from HTTP response header.
struct HTTPFileInfo
{
    // nullopt if the server doesn't report it.
    std::optional<size_t> file_size;
    std::optional<time_t> last_modified;
    bool seekable = false;

    /// If we got a non-retriable error, we cache that information and don't try HEAD requests again.
    std::optional<HTTPException> exception;
};


namespace detail
{
    /// Byte range, including right bound [begin, end].
    struct HTTPRange
    {
        std::optional<size_t> begin;
        std::optional<size_t> end;
    };

    template <typename UpdatableSessionPtr>
    class ReadWriteBufferFromHTTPBase : public SeekableReadBuffer, public WithFileName, public WithFileSize
    {
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
        const RemoteHostFilter * remote_host_filter = nullptr;
        std::function<void(size_t)> next_callback;

        size_t buffer_size;
        bool use_external_buffer;

        size_t offset_from_begin_pos = 0;
        HTTPRange read_range;
        std::optional<HTTPFileInfo> file_info;

        /// Delayed exception in case retries with partial content are not satisfiable.
        std::exception_ptr exception;
        bool retry_with_range_header = false;
        /// In case of redirects, save result uri to use it if we retry the request.
        std::optional<Poco::URI> saved_uri_redirect;

        ReadSettings settings;
        Poco::Logger * log;

        bool withPartialContent(const HTTPRange & range) const;

        size_t getOffset() const;

        void prepareRequest(Poco::Net::HTTPRequest & request, Poco::URI uri_, std::optional<HTTPRange> range) const;

        std::istream * callImpl(UpdatableSessionPtr & current_session, Poco::URI uri_, Poco::Net::HTTPResponse & response, const std::string & method_, bool for_object_info = false);

        size_t getFileSize() override;

        bool supportsReadAt() override;

        bool checkIfActuallySeekable() override;

        String getFileName() const override;

        enum class InitializeError
        {
            RETRYABLE_ERROR,
            /// If error is not retriable, `exception` variable must be set.
            NON_RETRYABLE_ERROR,
            NONE,
        };

        InitializeError initialization_error = InitializeError::NONE;

    private:
        void getHeadResponse(Poco::Net::HTTPResponse & response);

        void setupExternalBuffer();

    public:
        using NextCallback = std::function<void(size_t)>;
        using OutStreamCallback = std::function<void(std::ostream &)>;

        explicit ReadWriteBufferFromHTTPBase(
            UpdatableSessionPtr session_,
            Poco::URI uri_,
            const Poco::Net::HTTPBasicCredentials & credentials_,
            const std::string & method_ = {},
            OutStreamCallback out_stream_callback_ = {},
            size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
            const ReadSettings & settings_ = {},
            HTTPHeaderEntries http_header_entries_ = {},
            const RemoteHostFilter * remote_host_filter_ = nullptr,
            bool delay_initialization = false,
            bool use_external_buffer_ = false,
            std::optional<HTTPFileInfo> file_info_ = std::nullopt);

        void callWithRedirects(Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false, bool for_object_info = false);

        void call(UpdatableSessionPtr & current_session, Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false, bool for_object_info = false);

        /**
         * Throws if error is retryable, otherwise sets initialization_error = NON_RETRYABLE_ERROR and
         * saves exception into `exception` variable.
         */
        void initialize();

        bool nextImpl() override;

        size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) override;

        off_t getPosition() override;

        off_t seek(off_t offset_, int whence) override;

        void setReadUntilPosition(size_t until) override;

        void setReadUntilEnd() override;

        bool supportsRightBoundedReads() const override;

        // If true, if we destroy impl now, no work was wasted. Just for metrics.
        bool atEndOfRequestedRangeGuess();

        std::string getResponseCookie(const std::string & name, const std::string & def) const;

        /// Set function to call on each nextImpl, useful when you need to track
        /// progress.
        /// NOTE: parameter on each call is not incremental -- it's all bytes count
        /// passed through the buffer
        void setNextCallback(NextCallback next_callback_);

        const std::string & getCompressionMethod() const;

        std::optional<time_t> getLastModificationTime();

        HTTPFileInfo getFileInfo(bool use_cache = true);

        HTTPFileInfo parseFileInfo(const Poco::Net::HTTPResponse & response, size_t requested_range_begin);
    };
}


/// A short-lived HTTP session pool for one endpoint.
/// Keeps an unlimited number of sessions for one URI.
/// If URI changes (redirect), clears the pool.
/// If method is not GET or HEAD, we avoid pooling altogether, just in case.
/// The pool must outlive all session pointers created by it.
///
/// Session is only reused if it has HTTPSessionReusableTag attached. See comment in HTTPCommon.h
/// about HTTPSessionReusableTag.
class LocallyPooledSessionFactory
{
public:
    using Session = Poco::Net::HTTPClientSession;

    explicit LocallyPooledSessionFactory(const ConnectionTimeouts & timeouts_);

    struct Entry
    {
        Entry(LocallyPooledSessionFactory * pool_, Poco::URI uri_, HTTPSessionPtr session_) : pool(pool_), uri(uri_), session(session_) {}

        ~Entry()
        {
            if (pool)
                pool->returnSessionToPool(std::move(session), uri);
        }

        LocallyPooledSessionFactory * pool = nullptr;
        Poco::URI uri;
        HTTPSessionPtr session;
    };

    struct EntryPtr
    {
        std::shared_ptr<Entry> e;

        Session & operator*() { return *e->session; }
        Session * operator->() { return &*e->session; }
    };

    using SessionType = EntryPtr;

    /// Thread safe.
    SessionType buildNewSession(const Poco::URI & uri);

private:
    friend struct Entry;

    ConnectionTimeouts timeouts;

    std::mutex mutex;

    Poco::URI current_uri;
    std::vector<HTTPSessionPtr> available;

    void returnSessionToPool(HTTPSessionPtr s, Poco::URI uri);
};

/// Reuses HTTP sessions within the returned ReadBuffer (when doing seeks, retries, or random reads),
/// but doesn't share sessions with other buffers.
class ReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<LocallyPooledSessionFactory>>>
{
    using SessionType = UpdatableSession<LocallyPooledSessionFactory>;

public:
    explicit ReadWriteBufferFromHTTP(
        Poco::URI uri_,
        const std::string & method_,
        OutStreamCallback out_stream_callback_,
        const ConnectionTimeouts & timeouts_,
        const Poco::Net::HTTPBasicCredentials & credentials_,
        const UInt64 max_redirects = 0,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const ReadSettings & settings_ = {},
        const HTTPHeaderEntries & http_header_entries_ = {},
        const RemoteHostFilter * remote_host_filter_ = nullptr,
        bool delay_initialization_ = true,
        bool use_external_buffer_ = false);
};


/// Uses a global pool of sessions.
/// Currently not widely used because the eviction mechanism seems a little questionable: there's
/// no limit on number of endpoints in the pool, only a limit on number of sessions per endpoint.
class PooledSessionFactory
{
public:
    using SessionType = PooledHTTPSessionPtr;

    explicit PooledSessionFactory(
        const ConnectionTimeouts & timeouts_, size_t per_endpoint_pool_size_);

    /// Thread safe.
    SessionType buildNewSession(const Poco::URI & uri);

private:
    ConnectionTimeouts timeouts;
    size_t per_endpoint_pool_size;
};

class PooledReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<PooledSessionFactory>>>
{
    using SessionType = UpdatableSession<PooledSessionFactory>;
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<SessionType>>;

public:
    explicit PooledReadWriteBufferFromHTTP(
        Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback_ = {},
        const ConnectionTimeouts & timeouts_ = {},
        const Poco::Net::HTTPBasicCredentials & credentials_ = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const UInt64 max_redirects = 0,
        size_t max_connections_per_endpoint = DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT);
};


extern template class UpdatableSession<LocallyPooledSessionFactory>;
extern template class UpdatableSession<PooledSessionFactory>;
extern template class detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<LocallyPooledSessionFactory>>>;
extern template class detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<PooledSessionFactory>>>;

}
