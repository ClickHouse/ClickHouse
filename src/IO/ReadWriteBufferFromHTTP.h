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
    extern const int CANNOT_READ_FROM_ISTREAM;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int UNKNOWN_FILE_SIZE;
}

template <typename TSessionFactory>
class UpdatableSession
{
public:
    using SessionPtr = typename TSessionFactory::SessionType;

    explicit UpdatableSession(const Poco::URI & uri, UInt64 max_redirects_, std::shared_ptr<TSessionFactory> session_factory_)
        : max_redirects{max_redirects_}
        , initial_uri(uri)
        , session_factory(std::move(session_factory_))
    {
        session = session_factory->buildNewSession(uri);
    }

    SessionPtr getSession() { return session; }

    void updateSession(const Poco::URI & uri)
    {
        ++redirects;
        session = {};
        if (redirects <= max_redirects)
            session = session_factory->buildNewSession(uri);
        else
            throw Exception(ErrorCodes::TOO_MANY_REDIRECTS, "Too many redirects while trying to access {}", initial_uri.toString());
    }

    /// Thread safe.
    SessionPtr createDetachedSession(const Poco::URI & uri)
    {
        return session_factory->buildNewSession(uri);
    }

    std::shared_ptr<UpdatableSession<TSessionFactory>> clone(const Poco::URI & uri)
    {
        return std::make_shared<UpdatableSession<TSessionFactory>>(uri, max_redirects, session_factory);
    }
private:
    SessionPtr session;
    UInt64 redirects{0};
    UInt64 max_redirects;
    Poco::URI initial_uri;
    std::shared_ptr<TSessionFactory> session_factory;
};


namespace detail
{
    template <typename UpdatableSessionPtr>
    class ReadWriteBufferFromHTTPBase : public SeekableReadBuffer, public WithFileName, public WithFileSize
    {
    public:
        /// Information from HTTP response header.
        struct FileInfo
        {
            // nullopt if the server doesn't report it.
            std::optional<size_t> file_size;
            std::optional<time_t> last_modified;
            bool seekable = false;
        };

    protected:
        /// HTTP range, including right bound [begin, end].
        struct Range
        {
            std::optional<size_t> begin;
            std::optional<size_t> end;
        };

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
        std::optional<FileInfo> file_info;

        /// Delayed exception in case retries with partial content are not satisfiable.
        std::exception_ptr exception;
        bool retry_with_range_header = false;
        /// In case of redirects, save result uri to use it if we retry the request.
        std::optional<Poco::URI> saved_uri_redirect;

        bool http_skip_not_found_url;

        ReadSettings settings;
        Poco::Logger * log;

        bool withPartialContent(const Range & range) const
        {
            /**
             * Add range header if we have some passed range
             * or if we want to retry GET request on purpose.
             */
            return range.begin || range.end || retry_with_range_header;
        }

        size_t getRangeBegin() const { return read_range.begin.value_or(0); }

        size_t getOffset() const { return getRangeBegin() + offset_from_begin_pos; }

        void prepareRequest(Poco::Net::HTTPRequest & request, Poco::URI uri_, std::optional<Range> range) const
        {
            request.setHost(uri_.getHost()); // use original, not resolved host name in header

            if (out_stream_callback)
                request.setChunkedTransferEncoding(true);
            else if (method == Poco::Net::HTTPRequest::HTTP_POST)
                request.setContentLength(0);    /// No callback - no body

            for (auto & [header, value] : http_header_entries)
                request.set(header, value);

            if (range)
            {
                String range_header_value;
                if (range->end)
                    range_header_value = fmt::format("bytes={}-{}", *range->begin, *range->end);
                else
                    range_header_value = fmt::format("bytes={}-", *range->begin);
                LOG_TEST(log, "Adding header: Range: {}", range_header_value);
                request.set("Range", range_header_value);
            }

            if (!credentials.getUsername().empty())
                credentials.authenticate(request);
        }

        template <bool for_object_info = false>
        std::istream * callImpl(UpdatableSessionPtr & current_session, Poco::URI uri_, Poco::Net::HTTPResponse & response, const std::string & method_)
        {
            // With empty path poco will send "POST  HTTP/1.1" its bug.
            if (uri_.getPath().empty())
                uri_.setPath("/");

            std::optional<Range> range;
            if constexpr (!for_object_info)
            {
                if (withPartialContent(read_range))
                    range = Range{getOffset(), read_range.end};
            }

            Poco::Net::HTTPRequest request(method_, uri_.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            prepareRequest(request, uri_, range);

            LOG_TRACE(log, "Sending request to {}", uri_.toString());

            auto sess = current_session->getSession();
            try
            {
                auto & stream_out = sess->sendRequest(request);

                if (out_stream_callback)
                    out_stream_callback(stream_out);

                auto result_istr = receiveResponse(*sess, request, response, true);
                response.getCookies(cookies);

                /// we can fetch object info while the request is being processed
                /// and we don't want to override any context used by it
                if constexpr (!for_object_info)
                    content_encoding = response.get("Content-Encoding", "");

                return result_istr;
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
            if (!file_info)
                file_info = getFileInfo();

            if (file_info->file_size)
                return *file_info->file_size;

            throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", uri.toString());
        }

        bool supportsReadAt() override
        {
            if (!file_info)
                file_info = getFileInfo();
            return method == Poco::Net::HTTPRequest::HTTP_GET && file_info->seekable;
        }

        bool checkIfActuallySeekable() override
        {
            if (!file_info)
                file_info = getFileInfo();
            return file_info->seekable;
        }

        String getFileName() const override { return uri.toString(); }

        enum class InitializeError
        {
            RETRYABLE_ERROR,
            /// If error is not retriable, `exception` variable must be set.
            NON_RETRYABLE_ERROR,
            /// Allows to skip not found urls for globs
            SKIP_NOT_FOUND_URL,
            NONE,
        };

        InitializeError initialization_error = InitializeError::NONE;

    private:
        void getHeadResponse(Poco::Net::HTTPResponse & response)
        {
            for (size_t i = 0; i < settings.http_max_tries; ++i)
            {
                try
                {
                    callWithRedirects<true>(response, Poco::Net::HTTPRequest::HTTP_HEAD, true);
                    break;
                }
                catch (const Poco::Exception & e)
                {
                    if (i == settings.http_max_tries - 1 || !isRetriableError(response.getStatus()))
                        throw;

                    LOG_ERROR(log, "Failed to make HTTP_HEAD request to {}. Error: {}", uri.toString(), e.displayText());
                }
            }
        }

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
            const RemoteHostFilter * remote_host_filter_ = nullptr,
            bool delay_initialization = false,
            bool use_external_buffer_ = false,
            bool http_skip_not_found_url_ = false,
            std::optional<FileInfo> file_info_ = std::nullopt)
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
            , file_info(file_info_)
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
                [&user_agent](const HTTPHeaderEntry & entry) { return entry.name == user_agent; });

            if (iter == http_header_entries.end())
            {
                http_header_entries.emplace_back("User-Agent", fmt::format("ClickHouse/{}", VERSION_STRING));
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
            static constexpr std::array non_retriable_errors{
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_BAD_REQUEST,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_IMPLEMENTED,
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_METHOD_NOT_ALLOWED};

            return std::all_of(
                non_retriable_errors.begin(), non_retriable_errors.end(), [&](const auto status) { return http_status != status; });
        }

        static Poco::URI getUriAfterRedirect(const Poco::URI & prev_uri, Poco::Net::HTTPResponse & response)
        {
            auto location = response.get("Location");
            auto location_uri = Poco::URI(location);
            if (!location_uri.isRelative())
                return location_uri;
            /// Location header contains relative path. So we need to concatenate it
            /// with path from the original URI and normalize it.
            auto path = std::filesystem::weakly_canonical(std::filesystem::path(prev_uri.getPath()) / location);
            location_uri = prev_uri;
            location_uri.setPath(path);
            return location_uri;
        }

        template <bool for_object_info = false>
        void callWithRedirects(Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false)
        {
            UpdatableSessionPtr current_session = nullptr;

            /// we can fetch object info while the request is being processed
            /// and we don't want to override any context used by it
            if constexpr (for_object_info)
                current_session = session->clone(uri);
            else
                current_session = session;

            call<for_object_info>(current_session, response, method_, throw_on_all_errors);
            Poco::URI prev_uri = uri;

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect = getUriAfterRedirect(prev_uri, response);
                prev_uri = uri_redirect;
                if (remote_host_filter)
                    remote_host_filter->checkURL(uri_redirect);

                current_session->updateSession(uri_redirect);

                /// we can fetch object info while the request is being processed
                /// and we don't want to override any context used by it
                auto result_istr = callImpl<for_object_info>(current_session, uri_redirect, response, method);
                if constexpr (!for_object_info)
                    istr = result_istr;
            }
        }

        template <bool for_object_info = false>
        void call(UpdatableSessionPtr & current_session, Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors = false)
        {
            try
            {
                /// we can fetch object info while the request is being processed
                /// and we don't want to override any context used by it
                auto result_istr = callImpl<for_object_info>(current_session, saved_uri_redirect ? *saved_uri_redirect : uri, response, method_);
                if constexpr (!for_object_info)
                    istr = result_istr;
            }
            catch (...)
            {
                /// we can fetch object info while the request is being processed
                /// and we don't want to override any context used by it
                if constexpr (for_object_info)
                {
                    throw;
                }
                else
                {
                    if (throw_on_all_errors)
                        throw;

                    auto http_status = response.getStatus();

                    if (http_status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND && http_skip_not_found_url)
                    {
                        initialization_error = InitializeError::SKIP_NOT_FOUND_URL;
                    }
                    else if (!isRetriableError(http_status))
                    {
                        initialization_error = InitializeError::NON_RETRYABLE_ERROR;
                        exception = std::current_exception();
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        /**
         * Throws if error is retryable, otherwise sets initialization_error = NON_RETRYABLE_ERROR and
         * saves exception into `exception` variable. In case url is not found and skip_not_found_url == true,
         * sets initialization_error = SKIP_NOT_FOUND_URL, otherwise throws.
         */
        void initialize()
        {
            Poco::Net::HTTPResponse response;

            call(session, response, method);
            if (initialization_error != InitializeError::NONE)
                return;

            while (isRedirect(response.getStatus()))
            {
                Poco::URI uri_redirect = getUriAfterRedirect(saved_uri_redirect.value_or(uri), response);
                if (remote_host_filter)
                    remote_host_filter->checkURL(uri_redirect);

                session->updateSession(uri_redirect);

                istr = callImpl(session, uri_redirect, response, method);
                saved_uri_redirect = uri_redirect;
            }

            if (response.hasContentLength())
                LOG_DEBUG(log, "Received response with content length: {}", response.getContentLength());

            if (withPartialContent(read_range) && response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT)
            {
                /// Having `200 OK` instead of `206 Partial Content` is acceptable in case we retried with range.begin == 0.
                if (getOffset() != 0)
                {
                    if (!exception)
                    {
                        exception = std::make_exception_ptr(Exception(
                            ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                            "Cannot read with range: [{}, {}] (response status: {}, reason: {})",
                            *read_range.begin,
                            read_range.end ? toString(*read_range.end) : "-",
                            toString(response.getStatus()), response.getReason()));
                    }

                    /// Retry 200OK
                    if (response.getStatus() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
                        initialization_error = InitializeError::RETRYABLE_ERROR;
                    else
                        initialization_error = InitializeError::NON_RETRYABLE_ERROR;

                    return;
                }
                else if (read_range.end)
                {
                    /// We could have range.begin == 0 and range.end != 0 in case of DiskWeb and failing to read with partial content
                    /// will affect only performance, so a warning is enough.
                    LOG_WARNING(log, "Unable to read with range header: [{}, {}]", getRangeBegin(), *read_range.end);
                }
            }

            // Remember file size. It'll be used to report eof in next nextImpl() call.
            if (!read_range.end && response.hasContentLength())
                file_info = parseFileInfo(response, withPartialContent(read_range) ? getOffset() : 0);

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

            if ((read_range.end && getOffset() > read_range.end.value()) ||
                (file_info && file_info->file_size && getOffset() >= file_info->file_size.value()))
                return false;

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
            bool last_attempt = false;

            auto on_retriable_error = [&]()
            {
                retry_with_range_header = true;
                impl.reset();
                auto http_session = session->getSession();
                http_session->reset();
                if (!last_attempt)
                {
                    sleepForMilliseconds(milliseconds_to_wait);
                    milliseconds_to_wait = std::min(milliseconds_to_wait * 2, settings.http_retry_max_backoff_ms);
                }
            };

            for (size_t i = 0;; ++i)
            {
                if (last_attempt)
                    break;
                last_attempt = i + 1 >= settings.http_max_tries;

                exception = nullptr;
                initialization_error = InitializeError::NONE;

                try
                {
                    if (!impl)
                    {
                        initialize();

                        if (initialization_error == InitializeError::NON_RETRYABLE_ERROR)
                        {
                            assert(exception);
                            break;
                        }
                        else if (initialization_error == InitializeError::SKIP_NOT_FOUND_URL)
                        {
                            return false;
                        }
                        else if (initialization_error == InitializeError::RETRYABLE_ERROR)
                        {
                            LOG_ERROR(
                                log,
                                "HTTP request to `{}` failed at try {}/{} with bytes read: {}/{}. "
                                "(Current backoff wait is {}/{} ms)",
                                uri.toString(), i + 1, settings.http_max_tries, getOffset(),
                                read_range.end ? toString(*read_range.end) : "unknown",
                                milliseconds_to_wait, settings.http_retry_max_backoff_ms);

                            assert(exception);
                            on_retriable_error();
                            continue;
                        }

                        assert(!exception);

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
                    /// Don't reuse the session if its response istream errored out.
                    if (e.code() == ErrorCodes::CANNOT_READ_FROM_ISTREAM)
                    {
                        auto s = session->getSession();
                        s->attachSessionData(e.message());
                    }

                    /// Too many open files - non-retryable.
                    if (e.code() == POCO_EMFILE)
                        throw;

                    /** Retry request unconditionally if nothing has been read yet.
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

                    on_retriable_error();
                    exception = std::current_exception();
                }
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

        size_t readBigAt(char * to, size_t n, size_t offset) override
        {
            /// Caller must have checked supportsReadAt().
            /// This ensures we've sent at least one HTTP request and populated saved_uri_redirect.
            chassert(file_info && file_info->seekable);

            LOG_TEST(log, "readBigAt n: {}, offset: {}", n, offset);

            if (n == 0)
                return 0;

            Poco::URI uri_ = saved_uri_redirect.value_or(uri);
            if (uri_.getPath().empty())
                uri_.setPath("/");

            size_t milliseconds_to_wait = settings.http_retry_initial_backoff_ms;

            for (size_t attempt = 0;; ++attempt)
            {
                bool last_attempt = attempt + 1 >= settings.http_max_tries;

                Poco::Net::HTTPRequest request(method, uri_.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
                prepareRequest(request, uri_, Range { .begin = offset, .end = offset + n - 1});

                LOG_TRACE(log, "Sending request to {} for range [{}, {})", uri_.toString(), offset, offset + n);

                auto sess = session->createDetachedSession(uri_);

                Poco::Net::HTTPResponse response;
                std::istream * result_istr;

                try
                {
                    sess->sendRequest(request);
                    result_istr = receiveResponse(*sess, request, response, /*allow_redirects*/ false);

                    if (response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT &&
                        (offset != 0 || offset + n < *file_info->file_size))
                        throw Exception(
                            ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                            "Expected 206 Partial Content, got {} when reading {} range [{}, {})",
                            toString(response.getStatus()), uri_.toString(), offset, offset + n);

                    result_istr->read(to, n);
                    size_t gcount = result_istr->gcount();

                    if (gcount != n && !result_istr->eof())
                    {
                        /// Mark the session as not reusable.
                        sess->attachSessionData(std::string("istream gone bad"));

                        throw Exception(
                            ErrorCodes::CANNOT_READ_FROM_ISTREAM,
                            "{} at offset {} when reading HTTP response from {} for range [{}, {})",
                            result_istr->fail() ? "Cannot read from istream" : "Unexpected state of istream",
                            gcount, uri_.toString(), offset, offset + n);
                    }

                    return gcount;
                }
                catch (const Poco::Exception & e)
                {
                    sess->attachSessionData(e.message());

                    LOG_ERROR(
                        log,
                        "HTTP request (positioned) to `{}` with range [{}, {}) failed at try {}/{}: {}",
                        uri_.toString(), offset, offset + n, attempt + 1, settings.http_max_tries,
                        e.what());

                    /// Decide whether to retry.

                    if (last_attempt)
                        throw;

                    /// Too many open files - non-retryable.
                    if (e.code() == POCO_EMFILE)
                        throw;

                    if (auto h = dynamic_cast<const HTTPException*>(&e);
                        !isRetriableError(static_cast<Poco::Net::HTTPResponse::HTTPStatus>(h->getHTTPStatus())))
                        throw;

                    sleepForMilliseconds(milliseconds_to_wait);
                    milliseconds_to_wait = std::min(milliseconds_to_wait * 2, settings.http_retry_max_backoff_ms);
                    continue;
                }
            }
        }

        off_t getPosition() override { return getOffset() - available(); }

        off_t seek(off_t offset_, int whence) override
        {
            if (whence != SEEK_SET)
                throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

            if (offset_ < 0)
                throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}",
                    offset_);

            off_t current_offset = getOffset();
            if (!working_buffer.empty() && size_t(offset_) >= current_offset - working_buffer.size() && offset_ < current_offset)
            {
                pos = working_buffer.end() - (current_offset - offset_);
                assert(pos >= working_buffer.begin());
                assert(pos < working_buffer.end());

                return getPosition();
            }

            if (impl)
            {
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

                if (!atEndOfRequestedRangeGuess())
                    ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
                impl.reset();
            }

            resetWorkingBuffer();
            read_range.begin = offset_;
            offset_from_begin_pos = 0;

            return offset_;
        }

        void setReadUntilPosition(size_t until) override
        {
            until = std::max(until, 1ul);
            if (read_range.end && *read_range.end + 1 == until)
                return;
            read_range.end = until - 1;
            read_range.begin = getPosition();
            resetWorkingBuffer();
            if (impl)
            {
                if (!atEndOfRequestedRangeGuess())
                    ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
                impl.reset();
            }
        }

        void setReadUntilEnd() override
        {
            if (!read_range.end)
                return;
            read_range.end.reset();
            read_range.begin = getPosition();
            resetWorkingBuffer();
            if (impl)
            {
                if (!atEndOfRequestedRangeGuess())
                    ProfileEvents::increment(ProfileEvents::ReadBufferSeekCancelConnection);
                impl.reset();
            }
        }

        bool supportsRightBoundedReads() const override { return true; }

        // If true, if we destroy impl now, no work was wasted. Just for metrics.
        bool atEndOfRequestedRangeGuess()
        {
            if (!impl)
                return true;
            if (read_range.end)
                return getPosition() > static_cast<off_t>(*read_range.end);
            if (file_info && file_info->file_size)
                return getPosition() >= static_cast<off_t>(*file_info->file_size);
            return false;
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

        const std::string & getCompressionMethod() const { return content_encoding; }

        std::optional<time_t> getLastModificationTime()
        {
            return getFileInfo().last_modified;
        }

        FileInfo getFileInfo()
        {
            Poco::Net::HTTPResponse response;
            try
            {
                getHeadResponse(response);
            }
            catch (HTTPException & e)
            {
                /// Maybe the web server doesn't support HEAD requests.
                /// E.g. webhdfs reports status 400.
                /// We should proceed in hopes that the actual GET request will succeed.
                /// (Unless the error in transient. Don't want to nondeterministically sometimes
                /// fall back to slow whole-file reads when HEAD is actually supported; that sounds
                /// like a nightmare to debug.)
                if (e.getHTTPStatus() >= 400 && e.getHTTPStatus() <= 499 &&
                    e.getHTTPStatus() != Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS)
                    return FileInfo{};

                throw;
            }
            return parseFileInfo(response, 0);
        }

        FileInfo parseFileInfo(const Poco::Net::HTTPResponse & response, size_t requested_range_begin)
        {
            FileInfo res;

            if (response.hasContentLength())
            {
                res.file_size = response.getContentLength();

                if (response.getStatus() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT)
                {
                    *res.file_size += requested_range_begin;
                    res.seekable = true;
                }
                else
                {
                    res.seekable = response.has("Accept-Ranges") && response.get("Accept-Ranges") == "bytes";
                }
            }

            if (response.has("Last-Modified"))
            {
                String date_str = response.get("Last-Modified");
                struct tm info;
                char * end = strptime(date_str.data(), "%a, %d %b %Y %H:%M:%S %Z", &info);
                if (end == date_str.data() + date_str.size())
                    res.last_modified = timegm(&info);
            }

            return res;
        }
    };
}

class SessionFactory
{
public:
    explicit SessionFactory(const ConnectionTimeouts & timeouts_)
        : timeouts(timeouts_)
    {}

    using SessionType = HTTPSessionPtr;

    /// Thread safe.
    SessionType buildNewSession(const Poco::URI & uri) { return makeHTTPSession(uri, timeouts); }

private:
    ConnectionTimeouts timeouts;
};

class ReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<SessionFactory>>>
{
    using SessionType = UpdatableSession<SessionFactory>;
    using Parent = detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<SessionType>>;

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
        const RemoteHostFilter * remote_host_filter_ = nullptr,
        bool delay_initialization_ = true,
        bool use_external_buffer_ = false,
        bool skip_not_found_url_ = false,
        std::optional<FileInfo> file_info_ = std::nullopt)
        : Parent(
            std::make_shared<SessionType>(uri_, max_redirects, std::make_shared<SessionFactory>(timeouts)),
            uri_,
            credentials_,
            method_,
            out_stream_callback_,
            buffer_size_,
            settings_,
            http_header_entries_,
            remote_host_filter_,
            delay_initialization_,
            use_external_buffer_,
            skip_not_found_url_,
            file_info_)
    {
    }
};

class PooledSessionFactory
{
public:
    explicit PooledSessionFactory(
        const ConnectionTimeouts & timeouts_, size_t per_endpoint_pool_size_)
        : timeouts(timeouts_)
        , per_endpoint_pool_size(per_endpoint_pool_size_)
    {}

    using SessionType = PooledHTTPSessionPtr;

    /// Thread safe.
    SessionType buildNewSession(const Poco::URI & uri) { return makePooledHTTPSession(uri, timeouts, per_endpoint_pool_size); }

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
        size_t max_connections_per_endpoint = DEFAULT_COUNT_OF_HTTP_CONNECTIONS_PER_ENDPOINT)
        : Parent(
            std::make_shared<SessionType>(uri_, max_redirects, std::make_shared<PooledSessionFactory>(timeouts_, max_connections_per_endpoint)),
            uri_,
            credentials_,
            method_,
            out_stream_callback_,
            buffer_size_)
    {
    }
};

/// A short-lived HTTP session pool for one endpoint.
/// Keeps an unlimited number of sessions for one URI.
/// If URI changes (redirect), clears the pool.
/// If a session gets an error (indicated by attachSessionData()), it's removed from the pool.
/// The pool must outlive all session pointers created by it.
class LocallyPooledSessionFactory
{
public:
    explicit LocallyPooledSessionFactory(const ConnectionTimeouts & timeouts_)
        : timeouts(timeouts_) {}

    using Session = Poco::Net::HTTPClientSession;

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
    SessionType buildNewSession(const Poco::URI & uri)
    {
        {
            std::unique_lock lock(mutex);

            if (uri != current_uri)
            {
                available.clear();
                current_uri = uri;
            }

            if (!available.empty())
            {
                auto s = std::move(available.back());
                available.pop_back();
                return EntryPtr { .e = std::make_shared<Entry>(this, current_uri, s) };
            }
        }

        auto s = makeHTTPSession(uri, timeouts);
        s->setKeepAlive(true);
        return EntryPtr { .e = std::make_shared<Entry>(this, uri, s) };
    }

private:
    friend struct Entry;

    ConnectionTimeouts timeouts;

    std::mutex mutex;

    Poco::URI current_uri;
    std::vector<HTTPSessionPtr> available;

    void returnSessionToPool(HTTPSessionPtr s, Poco::URI uri)
    {
        std::unique_lock lock(mutex);

        if (uri != current_uri)
            return;

        const auto & session_data = s->sessionData();
        if (!session_data.empty())
        {
            auto msg = Poco::AnyCast<std::string>(session_data);
            if (!msg.empty())
            {
                LOG_TRACE(&Poco::Logger::get("LocallyPooledSessionFactory"),
                    "Not reusing session for {} because of error: {}", uri.toString(), msg);
                return;
            }
        }

        available.push_back(std::move(s));
    }
};

/// Reuses HTTP sessions within the returned ReadBuffer (when doing seeks, retries, or random reads),
/// but doesn't share sessions with other buffers.
/// Maybe this should be the default, replacing ReadWriteBufferFromHTTP.
class LocallyPooledReadWriteBufferFromHTTP : public detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<LocallyPooledSessionFactory>>>
{
    using SessionType = UpdatableSession<LocallyPooledSessionFactory>;

public:
    explicit LocallyPooledReadWriteBufferFromHTTP(
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
        bool use_external_buffer_ = false,
        bool skip_not_found_url_ = false,
        std::optional<FileInfo> file_info_ = std::nullopt,
        std::shared_ptr<LocallyPooledSessionFactory> session_pool = nullptr)
        : detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<SessionType>>(
            std::make_shared<SessionType>(uri_, max_redirects, session_pool ? session_pool : std::make_shared<LocallyPooledSessionFactory>(timeouts_)),
            uri_,
            credentials_,
            method_,
            out_stream_callback_,
            buffer_size_,
            settings_,
            http_header_entries_,
            remote_host_filter_,
            delay_initialization_,
            use_external_buffer_,
            skip_not_found_url_,
            file_info_) {}
};


class RangedReadWriteBufferFromHTTPFactory : public SeekableReadBufferFactory, public WithFileName
{
    using OutStreamCallback = ReadWriteBufferFromHTTP::OutStreamCallback;

public:
    RangedReadWriteBufferFromHTTPFactory(
        Poco::URI uri_,
        std::string method_,
        OutStreamCallback out_stream_callback_,
        ConnectionTimeouts timeouts_,
        const Poco::Net::HTTPBasicCredentials & credentials_,
        UInt64 max_redirects_ = 0,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        ReadSettings settings_ = {},
        HTTPHeaderEntries http_header_entries_ = {},
        const RemoteHostFilter * remote_host_filter_ = nullptr,
        bool delay_initialization_ = true,
        bool use_external_buffer_ = false,
        bool skip_not_found_url_ = false)
        : uri(uri_)
        , method(std::move(method_))
        , out_stream_callback(out_stream_callback_)
        , timeouts(std::move(timeouts_))
        , credentials(credentials_)
        , max_redirects(max_redirects_)
        , buffer_size(buffer_size_)
        , settings(std::move(settings_))
        , http_header_entries(std::move(http_header_entries_))
        , remote_host_filter(remote_host_filter_)
        , session_pool(std::make_shared<LocallyPooledSessionFactory>(timeouts))
        , delay_initialization(delay_initialization_)
        , use_external_buffer(use_external_buffer_)
        , skip_not_found_url(skip_not_found_url_)
    {
    }

    std::unique_ptr<SeekableReadBuffer> getReader() override
    {
        return std::make_unique<LocallyPooledReadWriteBufferFromHTTP>(
            uri,
            method,
            out_stream_callback,
            timeouts,
            credentials,
            max_redirects,
            buffer_size,
            settings,
            http_header_entries,
            remote_host_filter,
            delay_initialization,
            use_external_buffer,
            skip_not_found_url,
            file_info,
            session_pool);
    }

    size_t getFileSize() override
    {
        auto s = getFileInfo().file_size;
        if (!s)
            throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", uri.toString());
        return *s;
    }

    bool checkIfActuallySeekable() override
    {
        return getFileInfo().seekable;
    }

    LocallyPooledReadWriteBufferFromHTTP::FileInfo getFileInfo()
    {
        if (!file_info)
            file_info = static_cast<LocallyPooledReadWriteBufferFromHTTP*>(getReader().get())->getFileInfo();
        return *file_info;
    }

    String getFileName() const override { return uri.toString(); }

private:
    Poco::URI uri;
    std::string method;
    OutStreamCallback out_stream_callback;
    ConnectionTimeouts timeouts;
    const Poco::Net::HTTPBasicCredentials & credentials;
    UInt64 max_redirects;
    size_t buffer_size;
    ReadSettings settings;
    HTTPHeaderEntries http_header_entries;
    const RemoteHostFilter * remote_host_filter;
    std::shared_ptr<LocallyPooledSessionFactory> session_pool;
    std::optional<LocallyPooledReadWriteBufferFromHTTP::FileInfo> file_info;
    bool delay_initialization;
    bool use_external_buffer;
    bool skip_not_found_url;
};

}
