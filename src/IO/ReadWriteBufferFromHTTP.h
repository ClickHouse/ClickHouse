#pragma once

#include <functional>
#include <Common/RangeGenerator.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WithFileName.h>
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
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/RemoteHostFilter.h>
#include <Common/config.h>
#include <Common/config_version.h>


namespace ProfileEvents
{
extern const Event ReadBufferSeekCancelConnection;
}

namespace DB
{
/** Perform HTTP POST request and provide response to read.
  */

namespace ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

template <typename SessionPtr>
class UpdatableSessionBase
{
protected:
    SessionPtr session;
    UInt64 redirects{0};
    Poco::URI initial_uri;
    ConnectionTimeouts timeouts;
    UInt64 max_redirects;

public:
    virtual void buildNewSession(const Poco::URI & uri) = 0;

    explicit UpdatableSessionBase(const Poco::URI uri, const ConnectionTimeouts & timeouts_, UInt64 max_redirects_)
        : initial_uri{uri}, timeouts{timeouts_}, max_redirects{max_redirects_}
    {
    }

    SessionPtr getSession() { return session; }

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
    class ReadWriteBufferFromHTTPBase : public SeekableReadBuffer, public WithFileName, public WithFileSize
    {
    public:
        using HTTPHeaderEntry = std::tuple<std::string, std::string>;
        using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>;

        /// HTTP range, including right bound [begin, end].
        struct Range
        {
            std::optional<size_t> begin;
            std::optional<size_t> end;
        };

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
        Range read_range;

        /// Delayed exception in case retries with partial content are not satisfiable.
        std::exception_ptr exception;
        bool retry_with_range_header = false;
        /// In case of redirects, save result uri to use it if we retry the request.
        std::optional<Poco::URI> saved_uri_redirect;

        bool http_skip_not_found_url;

        ReadSettings settings;
        Poco::Logger * log;

        bool withPartialContent() const
        {
            /**
             * Add range header if we have some passed range (for disk web)
             * or if we want to retry GET request on purpose.
             */
            return read_range.begin || read_range.end || retry_with_range_header;
        }

        size_t getRangeBegin() const { return read_range.begin.value_or(0); }

        size_t getOffset() const { return getRangeBegin() + offset_from_begin_pos; }

        std::istream * callImpl(Poco::URI uri_, Poco::Net::HTTPResponse & response, const std::string & method_)
        {
            // With empty path poco will send "POST  HTTP/1.1" its bug.
            if (uri_.getPath().empty())
                uri_.setPath("/");

            Poco::Net::HTTPRequest request(method_, uri_.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            request.setHost(uri_.getHost()); // use original, not resolved host name in header

            if (out_stream_callback)
                request.setChunkedTransferEncoding(true);

            for (auto & http_header_entry : http_header_entries)
                request.set(std::get<0>(http_header_entry), std::get<1>(http_header_entry));

            if (withPartialContent())
            {
                String range_header_value;
                if (read_range.end)
                    range_header_value = fmt::format("bytes={}-{}", getOffset(), *read_range.end);
                else
                    range_header_value = fmt::format("bytes={}-", getOffset());
                LOG_TEST(log, "Adding header: Range: {}", range_header_value);
                request.set("Range", range_header_value);
            }

            if (!credentials.getUsername().empty())
                credentials.authenticate(request);

            LOG_TRACE(log, "Sending request to {}", uri_.toString());

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

        size_t getFileSize() override
        {
            if (read_range.end)
                return *read_range.end - getRangeBegin();

            Poco::Net::HTTPResponse response;
            for (size_t i = 0; i < 10; ++i)
            {
                try
                {
                    callWithRedirects(response, Poco::Net::HTTPRequest::HTTP_HEAD);
                    break;
                }
                catch (const Poco::Exception & e)
                {
                    LOG_ERROR(log, "Failed to make HTTP_HEAD request to {}. Error: {}", uri.toString(), e.displayText());
                }
            }

            if (response.hasContentLength())
                read_range.end = getRangeBegin() + response.getContentLength();

            return *read_range.end;
        }

        String getFileName() const override { return uri.toString(); }

        enum class InitializeError
        {
            /// If error is not retriable, `exception` variable must be set.
            NON_RETRIABLE_ERROR,
            /// Allows to skip not found urls for globs
            SKIP_NOT_FOUND_URL,
            NONE,
        };

        InitializeError initialization_error = InitializeError::NONE;

    private:
        void setupExternalBuffer()
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
            Range read_range_ = {},
            const RemoteHostFilter * remote_host_filter_ = nullptr,
            bool delay_initialization = false,
            bool use_external_buffer_ = false,
            bool http_skip_not_found_url_ = false)
            : SeekableReadBuffer(nullptr, 0)
            , uri {uri_}
            , method {!method_.empty() ? method_ : out_stream_callback_ ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET}
            , session {session_}
            , out_stream_callback {out_stream_callback_}
            , credentials {credentials_}
            , http_header_entries {std::move(http_header_entries_)}
            , remote_host_filter {remote_host_filter_}
            , buffer_size {buffer_size_}
            , use_external_buffer {use_external_buffer_}
            , read_range(read_range_)
            , http_skip_not_found_url(http_skip_not_found_url_)
            , settings {settings_}
            , log(&Poco::Logger::get("ReadWriteBufferFromHTTP"))
        {
            if (settings.http_max_tries <= 0 || settings.http_retry_initial_backoff_ms <= 0
                || settings.http_retry_initial_backoff_ms >= settings.http_retry_max_backoff_ms)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid setting for http backoff, "
                    "must be http_max_tries >= 1 (current is {}) and "
                    "0 < http_retry_initial_backoff_ms < settings.http_retry_max_backoff_ms (now 0 < {} < {})",
                    settings.http_max_tries,
                    settings.http_retry_initial_backoff_ms,
                    settings.http_retry_max_backoff_ms);

            // Configure User-Agent if it not already set.
            const std::string user_agent = "User-Agent";
            auto iter = std::find_if(
                http_header_entries.begin(),
                http_header_entries.end(),
                [&user_agent](const HTTPHeaderEntry & entry) { return std::get<0>(entry) == user_agent; });

            if (iter == http_header_entries.end())
            {
                http_header_entries.emplace_back(std::make_pair("User-Agent", fmt::format("ClickHouse/{}", VERSION_STRING)));
            }

            if (!delay_initialization)
            {
                initialize();
                if (exception)
                    std::rethrow_exception(exception);
            }
        }

        static bool isRetriableError(const Poco::Net::HTTPResponse::HTTPStatus http_status) noexcept
        {
            constexpr std::array non_retriable_errors{
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_BAD_REQUEST,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_METHOD_NOT_ALLOWED};

            return std::all_of(
                non_retriable_errors.begin(), non_retriable_errors.end(), [&](const auto status) { return http_status != status; });
        }

        void callWithRedirects(Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false)
        {
            call(response, method_, throw_on_all_errors);

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect(response.get("Location"));
                if (remote_host_filter)
                    remote_host_filter->checkURL(uri_redirect);

                session->updateSession(uri_redirect);

                istr = callImpl(uri_redirect, response, method);
            }
        }

        void call(Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false)
        {
            try
            {
                istr = callImpl(saved_uri_redirect ? *saved_uri_redirect : uri, response, method_);
            }
            catch (...)
            {
                if (throw_on_all_errors)
                {
                    throw;
                }

                auto http_status = response.getStatus();

                if (http_status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND && http_skip_not_found_url)
                {
                    initialization_error = InitializeError::SKIP_NOT_FOUND_URL;
                }
                else if (!isRetriableError(http_status))
                {
                    initialization_error = InitializeError::NON_RETRIABLE_ERROR;
                    exception = std::current_exception();
                }
                else
                {
                    throw;
                }
            }
        }

        /**
         * Throws if error is retriable, otherwise sets initialization_error = NON_RETRIABLE_ERROR and
         * saves exception into `exception` variable. In case url is not found and skip_not_found_url == true,
         * sets initialization_error = SKIP_NOT_FOUND_URL, otherwise throws.
         */
        void initialize()
        {
            Poco::Net::HTTPResponse response;

            call(response, method);
            if (initialization_error != InitializeError::NONE)
                return;

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect(response.get("Location"));
                if (remote_host_filter)
                    remote_host_filter->checkURL(uri_redirect);

                session->updateSession(uri_redirect);

                istr = callImpl(uri_redirect, response, method);
                saved_uri_redirect = uri_redirect;
            }

            if (withPartialContent() && response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT)
            {
                /// Having `200 OK` instead of `206 Partial Content` is acceptable in case we retried with range.begin == 0.
                if (read_range.begin && *read_range.begin != 0)
                {
                    if (!exception)
                        exception = std::make_exception_ptr(Exception(
                            ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                            "Cannot read with range: [{}, {}]",
                            *read_range.begin,
                            read_range.end ? *read_range.end : '-'));

                    initialization_error = InitializeError::NON_RETRIABLE_ERROR;
                    return;
                }
                else if (read_range.end)
                {
                    /// We could have range.begin == 0 and range.end != 0 in case of DiskWeb and failing to read with partial content
                    /// will affect only performance, so a warning is enough.
                    LOG_WARNING(log, "Unable to read with range header: [{}, {}]", getRangeBegin(), *read_range.end);
                }
            }

            if (!offset_from_begin_pos && !read_range.end && response.hasContentLength())
                read_range.end = getRangeBegin() + response.getContentLength();

            try
            {
                impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size);

                if (use_external_buffer)
                {
                    setupExternalBuffer();
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
            if (initialization_error == InitializeError::SKIP_NOT_FOUND_URL)
                return false;
            assert(initialization_error == InitializeError::NONE);

            if (next_callback)
                next_callback(count());

            if (read_range.end && getOffset() > read_range.end.value())
            {
                assert(getOffset() == read_range.end.value() + 1);
                return false;
            }

            if (impl)
            {
                if (use_external_buffer)
                {
                    setupExternalBuffer();
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
            }

            bool result = false;
            size_t milliseconds_to_wait = settings.http_retry_initial_backoff_ms;

            for (size_t i = 0; i < settings.http_max_tries; ++i)
            {
                try
                {
                    if (!impl)
                    {
                        initialize();
                        if (initialization_error == InitializeError::NON_RETRIABLE_ERROR)
                        {
                            assert(exception);
                            break;
                        }
                        else if (initialization_error == InitializeError::SKIP_NOT_FOUND_URL)
                        {
                            return false;
                        }

                        if (use_external_buffer)
                        {
                            setupExternalBuffer();
                        }
                    }

                    result = impl->next();
                    exception = nullptr;
                    break;
                }
                catch (const Poco::Exception & e)
                {
                    /**
                     * Retry request unconditionally if nothing has been read yet.
                     * Otherwise if it is GET method retry with range header.
                     */
                    bool can_retry_request = !offset_from_begin_pos || method == Poco::Net::HTTPRequest::HTTP_GET;
                    if (!can_retry_request)
                        throw;

                    LOG_ERROR(
                        log,
                        "HTTP request to `{}` failed at try {}/{} with bytes read: {}/{}. "
                        "Error: {}. (Current backoff wait is {}/{} ms)",
                        uri.toString(),
                        i + 1,
                        settings.http_max_tries,
                        getOffset(),
                        read_range.end ? toString(*read_range.end) : "unknown",
                        e.displayText(),
                        milliseconds_to_wait,
                        settings.http_retry_max_backoff_ms);

                    retry_with_range_header = true;
                    exception = std::current_exception();
                    impl.reset();
                    auto http_session = session->getSession();
                    http_session->reset();
                    sleepForMilliseconds(milliseconds_to_wait);
                }

                milliseconds_to_wait = std::min(milliseconds_to_wait * 2, settings.http_retry_max_backoff_ms);
            }

            if (exception)
                std::rethrow_exception(exception);

            if (!result)
                return false;

            internal_buffer = impl->buffer();
            working_buffer = internal_buffer;
            offset_from_begin_pos += working_buffer.size();
            return true;
        }

        off_t getPosition() override { return getOffset() - available(); }

        off_t seek(off_t offset_, int whence) override
        {
            if (whence != SEEK_SET)
                throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            if (offset_ < 0)
                throw Exception(
                    "Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

            off_t current_offset = getOffset();
            if (!working_buffer.empty() && size_t(offset_) >= current_offset - working_buffer.size() && offset_ < current_offset)
            {
                pos = working_buffer.end() - (current_offset - offset_);
                assert(pos >= working_buffer.begin());
                assert(pos <= working_buffer.end());

                return getPosition();
            }

            auto position = getPosition();
            if (offset_ > position)
            {
                size_t diff = offset_ - position;
                if (diff < settings.remote_read_min_bytes_for_seek)
                {
                    ignore(diff);
                    return offset_;
                }
            }

            if (impl)
            {
                ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
                impl.reset();
            }

            resetWorkingBuffer();
            read_range.begin = offset_;
            read_range.end = std::nullopt;
            offset_from_begin_pos = 0;

            return offset_;
        }

        SeekableReadBuffer::Range getRemainingReadRange() const override { return {getOffset(), read_range.end}; }

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

        const std::string & getCompressionMethod() const { return content_encoding; }
    };
}

class UpdatableSession : public UpdatableSessionBase<HTTPSessionPtr>
{
    using Parent = UpdatableSessionBase<HTTPSessionPtr>;

public:
    UpdatableSession(const Poco::URI uri, const ConnectionTimeouts & timeouts_, const UInt64 max_redirects_)
        : Parent(uri, timeouts_, max_redirects_)
    {
        session = makeHTTPSession(initial_uri, timeouts);
    }

    void buildNewSession(const Poco::URI & uri) override { session = makeHTTPSession(uri, timeouts); }
};

class ReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession>>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession>>;

public:
    ReadWriteBufferFromHTTP(
        Poco::URI uri_,
        const std::string & method_,
        OutStreamCallback out_stream_callback_,
        const ConnectionTimeouts & timeouts,
        const Poco::Net::HTTPBasicCredentials & credentials_,
        const UInt64 max_redirects = 0,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const ReadSettings & settings_ = {},
        const HTTPHeaderEntries & http_header_entries_ = {},
        Range read_range_ = {},
        const RemoteHostFilter * remote_host_filter_ = nullptr,
        bool delay_initialization_ = true,
        bool use_external_buffer_ = false,
        bool skip_not_found_url_ = false)
        : Parent(
            std::make_shared<UpdatableSession>(uri_, timeouts, max_redirects),
            uri_,
            credentials_,
            method_,
            out_stream_callback_,
            buffer_size_,
            settings_,
            http_header_entries_,
            read_range_,
            remote_host_filter_,
            delay_initialization_,
            use_external_buffer_,
            skip_not_found_url_)
    {
    }
};

class RangedReadWriteBufferFromHTTPFactory : public ParallelReadBuffer::ReadBufferFactory, public WithFileName
{
    using OutStreamCallback = ReadWriteBufferFromHTTP::OutStreamCallback;

public:
    RangedReadWriteBufferFromHTTPFactory(
        size_t total_object_size_,
        size_t range_step_,
        Poco::URI uri_,
        std::string method_,
        OutStreamCallback out_stream_callback_,
        ConnectionTimeouts timeouts_,
        const Poco::Net::HTTPBasicCredentials & credentials_,
        UInt64 max_redirects_ = 0,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        ReadSettings settings_ = {},
        ReadWriteBufferFromHTTP::HTTPHeaderEntries http_header_entries_ = {},
        const RemoteHostFilter * remote_host_filter_ = nullptr,
        bool delay_initialization_ = true,
        bool use_external_buffer_ = false,
        bool skip_not_found_url_ = false)
        : range_generator(total_object_size_, range_step_)
        , total_object_size(total_object_size_)
        , range_step(range_step_)
        , uri(uri_)
        , method(std::move(method_))
        , out_stream_callback(out_stream_callback_)
        , timeouts(std::move(timeouts_))
        , credentials(credentials_)
        , max_redirects(max_redirects_)
        , buffer_size(buffer_size_)
        , settings(std::move(settings_))
        , http_header_entries(std::move(http_header_entries_))
        , remote_host_filter(remote_host_filter_)
        , delay_initialization(delay_initialization_)
        , use_external_buffer(use_external_buffer_)
        , skip_not_found_url(skip_not_found_url_)
    {
    }

    SeekableReadBufferPtr getReader() override
    {
        const auto next_range = range_generator.nextRange();
        if (!next_range)
        {
            return nullptr;
        }

        return std::make_shared<ReadWriteBufferFromHTTP>(
            uri,
            method,
            out_stream_callback,
            timeouts,
            credentials,
            max_redirects,
            buffer_size,
            settings,
            http_header_entries,
            // HTTP Range has inclusive bounds, i.e. [from, to]
            ReadWriteBufferFromHTTP::Range{next_range->first, next_range->second - 1},
            remote_host_filter,
            delay_initialization,
            use_external_buffer,
            skip_not_found_url);
    }

    off_t seek(off_t off, [[maybe_unused]] int whence) override
    {
        range_generator = RangeGenerator{total_object_size, range_step, static_cast<size_t>(off)};
        return off;
    }

    size_t getFileSize() override { return total_object_size; }

    String getFileName() const override { return uri.toString(); }

private:
    RangeGenerator range_generator;
    size_t total_object_size;
    size_t range_step;
    Poco::URI uri;
    std::string method;
    OutStreamCallback out_stream_callback;
    ConnectionTimeouts timeouts;
    const Poco::Net::HTTPBasicCredentials & credentials;
    UInt64 max_redirects;
    size_t buffer_size;
    ReadSettings settings;
    ReadWriteBufferFromHTTP::HTTPHeaderEntries http_header_entries;
    const RemoteHostFilter * remote_host_filter;
    bool delay_initialization;
    bool use_external_buffer;
    bool skip_not_found_url;
};

class UpdatablePooledSession : public UpdatableSessionBase<PooledHTTPSessionPtr>
{
    using Parent = UpdatableSessionBase<PooledHTTPSessionPtr>;

private:
    size_t per_endpoint_pool_size;

public:
    explicit UpdatablePooledSession(
        const Poco::URI uri, const ConnectionTimeouts & timeouts_, const UInt64 max_redirects_, size_t per_endpoint_pool_size_)
        : Parent(uri, timeouts_, max_redirects_), per_endpoint_pool_size{per_endpoint_pool_size_}
    {
        session = makePooledHTTPSession(initial_uri, timeouts, per_endpoint_pool_size);
    }

    void buildNewSession(const Poco::URI & uri) override { session = makePooledHTTPSession(uri, timeouts, per_endpoint_pool_size); }
};

class PooledReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatablePooledSession>>
{
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatablePooledSession>>;

public:
    explicit PooledReadWriteBufferFromHTTP(
        Poco::URI uri_,
        const std::string & method_ = {},
        OutStreamCallback out_stream_callback_ = {},
        const ConnectionTimeouts & timeouts_ = {},
        const Poco::Net::HTTPBasicCredentials & credentials_ = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        const UInt64 max_redirects = 0,
        size_t max_connections_per_endpoint = DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT)
        : Parent(
            std::make_shared<UpdatablePooledSession>(uri_, timeouts_, max_redirects, max_connections_per_endpoint),
            uri_,
            credentials_,
            method_,
            out_stream_callback_,
            buffer_size_)
    {
    }
};

}
