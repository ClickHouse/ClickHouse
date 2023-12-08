#include "ReadWriteBufferFromHTTP.h"

#include <IO/HTTPCommon.h>

namespace ProfileEvents
{
extern const Event ReadBufferSeekCancelConnection;
extern const Event ReadWriteBufferFromHTTPPreservedSessions;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_REDIRECTS;
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int UNKNOWN_FILE_SIZE;
}

template <typename TSessionFactory>
UpdatableSession<TSessionFactory>::UpdatableSession(const Poco::URI & uri, UInt64 max_redirects_, std::shared_ptr<TSessionFactory> session_factory_)
    : max_redirects{max_redirects_}
    , initial_uri(uri)
    , session_factory(std::move(session_factory_))
{
    session = session_factory->buildNewSession(uri);
}

template <typename TSessionFactory>
typename UpdatableSession<TSessionFactory>::SessionPtr UpdatableSession<TSessionFactory>::getSession() { return session; }

template <typename TSessionFactory>
void UpdatableSession<TSessionFactory>::updateSession(const Poco::URI & uri)
{
    ++redirects;
    if (redirects <= max_redirects)
        session = session_factory->buildNewSession(uri);
    else
        throw Exception(ErrorCodes::TOO_MANY_REDIRECTS,
            "Too many redirects while trying to access {}."
            " You can {} redirects by changing the setting 'max_http_get_redirects'."
            " Example: `SET max_http_get_redirects = 10`."
            " Redirects are restricted to prevent possible attack when a malicious server redirects to an internal resource, bypassing the authentication or firewall.",
            initial_uri.toString(), max_redirects ? "increase the allowed maximum number of" : "allow");
}

template <typename TSessionFactory>
typename UpdatableSession<TSessionFactory>::SessionPtr UpdatableSession<TSessionFactory>::createDetachedSession(const Poco::URI & uri)
{
    return session_factory->buildNewSession(uri);
}

template <typename TSessionFactory>
std::shared_ptr<UpdatableSession<TSessionFactory>> UpdatableSession<TSessionFactory>::clone(const Poco::URI & uri)
{
    return std::make_shared<UpdatableSession<TSessionFactory>>(uri, max_redirects, session_factory);
}


namespace detail
{

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

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::withPartialContent(const HTTPRange & range) const
{
    /**
     * Add range header if we have some passed range
     * or if we want to retry GET request on purpose.
     */
    return range.begin || range.end || retry_with_range_header;
}

template <typename UpdatableSessionPtr>
size_t ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getOffset() const { return read_range.begin.value_or(0) + offset_from_begin_pos; }

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::prepareRequest(Poco::Net::HTTPRequest & request, Poco::URI uri_, std::optional<HTTPRange> range) const
{
    request.setHost(uri_.getHost()); // use original, not resolved host name in header

    if (out_stream_callback)
        request.setChunkedTransferEncoding(true);
    else if (method == Poco::Net::HTTPRequest::HTTP_POST)
        request.setContentLength(0);    /// No callback - no body

    for (const auto & [header, value] : http_header_entries)
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

template <typename UpdatableSessionPtr>
std::istream * ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::callImpl(
    UpdatableSessionPtr & current_session, Poco::URI uri_, Poco::Net::HTTPResponse & response, const std::string & method_, bool for_object_info)
{
    // With empty path poco will send "POST  HTTP/1.1" its bug.
    if (uri_.getPath().empty())
        uri_.setPath("/");

    std::optional<HTTPRange> range;
    if (!for_object_info)
    {
        if (withPartialContent(read_range))
            range = HTTPRange{getOffset(), read_range.end};
    }

    Poco::Net::HTTPRequest request(method_, uri_.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    prepareRequest(request, uri_, range);

    LOG_TRACE(log, "Sending request to {}", uri_.toString());

    auto sess = current_session->getSession();
    auto & stream_out = sess->sendRequest(request);

    if (out_stream_callback)
        out_stream_callback(stream_out);

    auto result_istr = receiveResponse(*sess, request, response, true);
    response.getCookies(cookies);

    /// we can fetch object info while the request is being processed
    /// and we don't want to override any context used by it
    if (!for_object_info)
        content_encoding = response.get("Content-Encoding", "");

    return result_istr;
}

template <typename UpdatableSessionPtr>
size_t ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getFileSize()
{
    if (!file_info)
        file_info = getFileInfo();

    if (file_info->file_size)
        return *file_info->file_size;

    throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out file size for: {}", uri.toString());
}

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::supportsReadAt()
{
    if (!file_info)
        file_info = getFileInfo();
    return method == Poco::Net::HTTPRequest::HTTP_GET && file_info->seekable;
}

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::checkIfActuallySeekable()
{
    if (!file_info)
        file_info = getFileInfo();
    return file_info->seekable;
}

template <typename UpdatableSessionPtr>
String ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getFileName() const { return uri.toString(); }

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getHeadResponse(Poco::Net::HTTPResponse & response)
{
    for (size_t i = 0; i < settings.http_max_tries; ++i)
    {
        try
        {
            callWithRedirects(response, Poco::Net::HTTPRequest::HTTP_HEAD, true, true);
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

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::setupExternalBuffer()
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

template <typename UpdatableSessionPtr>
ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::ReadWriteBufferFromHTTPBase(
    UpdatableSessionPtr session_,
    Poco::URI uri_,
    const Poco::Net::HTTPBasicCredentials & credentials_,
    const std::string & method_,
    OutStreamCallback out_stream_callback_,
    size_t buffer_size_,
    const ReadSettings & settings_,
    HTTPHeaderEntries http_header_entries_,
    const RemoteHostFilter * remote_host_filter_,
    bool delay_initialization,
    bool use_external_buffer_,
    bool http_skip_not_found_url_,
    std::optional<HTTPFileInfo> file_info_,
    Poco::Net::HTTPClientSession::ProxyConfig proxy_config_)
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
    , proxy_config(proxy_config_)
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

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::callWithRedirects(Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors, bool for_object_info)
{
    UpdatableSessionPtr current_session = nullptr;

    /// we can fetch object info while the request is being processed
    /// and we don't want to override any context used by it
    if (for_object_info)
        current_session = session->clone(uri);
    else
        current_session = session;

    call(current_session, response, method_, throw_on_all_errors, for_object_info);
    saved_uri_redirect = uri;

    while (isRedirect(response.getStatus()))
    {
        Poco::URI uri_redirect = getUriAfterRedirect(*saved_uri_redirect, response);
        saved_uri_redirect = uri_redirect;
        if (remote_host_filter)
            remote_host_filter->checkURL(uri_redirect);

        current_session->updateSession(uri_redirect);

        /// we can fetch object info while the request is being processed
        /// and we don't want to override any context used by it
        auto result_istr = callImpl(current_session, uri_redirect, response, method, for_object_info);
        if (!for_object_info)
            istr = result_istr;
    }
}

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::call(UpdatableSessionPtr & current_session, Poco::Net::HTTPResponse & response, const String & method_, bool throw_on_all_errors, bool for_object_info)
{
    try
    {
        /// we can fetch object info while the request is being processed
        /// and we don't want to override any context used by it
        auto result_istr = callImpl(current_session, saved_uri_redirect ? *saved_uri_redirect : uri, response, method_, for_object_info);
        if (!for_object_info)
            istr = result_istr;
    }
    catch (...)
    {
        /// we can fetch object info while the request is being processed
        /// and we don't want to override any context used by it
        if (for_object_info)
            throw;

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

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::initialize()
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
            LOG_WARNING(log, "Unable to read with range header: [{}, {}]", read_range.begin.value_or(0), *read_range.end);
        }
    }

    // Remember file size. It'll be used to report eof in next nextImpl() call.
    if (!read_range.end && response.hasContentLength())
        file_info = parseFileInfo(response, withPartialContent(read_range) ? getOffset() : 0);

    impl = std::make_unique<ReadBufferFromIStream>(*istr, buffer_size);

    if (use_external_buffer)
        setupExternalBuffer();
}

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::nextImpl()
{
    if (initialization_error == InitializeError::SKIP_NOT_FOUND_URL)
        return false;
    assert(initialization_error == InitializeError::NONE);

    if (next_callback)
        next_callback(count());

    if ((read_range.end && getOffset() > read_range.end.value()) ||
        (file_info && file_info->file_size && getOffset() >= file_info->file_size.value()))
    {
        /// Response was fully read.
        markSessionForReuse(session->getSession());
        ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPPreservedSessions);
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
    {
        /// Eof is reached, i.e response was fully read.
        markSessionForReuse(session->getSession());
        ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPPreservedSessions);
        return false;
    }

    internal_buffer = impl->buffer();
    working_buffer = internal_buffer;
    offset_from_begin_pos += working_buffer.size();
    return true;
}

template <typename UpdatableSessionPtr>
size_t ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback)
{
    /// Caller must have checked supportsReadAt().
    /// This ensures we've sent at least one HTTP request and populated saved_uri_redirect.
    chassert(file_info && file_info->seekable);

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
        prepareRequest(request, uri_, HTTPRange { .begin = offset, .end = offset + n - 1});

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

            bool cancelled;
            size_t r = copyFromIStreamWithProgressCallback(*result_istr, to, n, progress_callback, &cancelled);

            if (!cancelled)
            {
                /// Response was fully read.
                markSessionForReuse(sess);
                ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPPreservedSessions);
            }

            return r;
        }
        catch (const Poco::Exception & e)
        {
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

            if (const auto * h = dynamic_cast<const HTTPException*>(&e);
                h && !isRetriableError(static_cast<Poco::Net::HTTPResponse::HTTPStatus>(h->getHTTPStatus())))
                throw;

            sleepForMilliseconds(milliseconds_to_wait);
            milliseconds_to_wait = std::min(milliseconds_to_wait * 2, settings.http_retry_max_backoff_ms);
            continue;
        }
    }
}

template <typename UpdatableSessionPtr>
off_t ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getPosition() { return getOffset() - available(); }

template <typename UpdatableSessionPtr>
off_t ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::seek(off_t offset_, int whence)
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

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::setReadUntilPosition(size_t until)
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

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::setReadUntilEnd()
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

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::supportsRightBoundedReads() const { return true; }

template <typename UpdatableSessionPtr>
bool ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::atEndOfRequestedRangeGuess()
{
    if (!impl)
        return true;
    if (read_range.end)
        return getPosition() > static_cast<off_t>(*read_range.end);
    if (file_info && file_info->file_size)
        return getPosition() >= static_cast<off_t>(*file_info->file_size);
    return false;
}

template <typename UpdatableSessionPtr>
std::string ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getResponseCookie(const std::string & name, const std::string & def) const
{
    for (const auto & cookie : cookies)
        if (cookie.getName() == name)
            return cookie.getValue();
    return def;
}

template <typename UpdatableSessionPtr>
void ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::setNextCallback(NextCallback next_callback_)
{
    next_callback = next_callback_;
    /// Some data maybe already read
    next_callback(count());
}

template <typename UpdatableSessionPtr>
const std::string & ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getCompressionMethod() const { return content_encoding; }

template <typename UpdatableSessionPtr>
std::optional<time_t> ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::tryGetLastModificationTime()
{
    if (!file_info)
    {
        try
        {
            file_info = getFileInfo();
        }
        catch (...)
        {
            return std::nullopt;
        }
    }

    return file_info->last_modified;
}

template <typename UpdatableSessionPtr>
HTTPFileInfo ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::getFileInfo()
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
            return HTTPFileInfo{};

        throw;
    }
    return parseFileInfo(response, 0);
}

template <typename UpdatableSessionPtr>
HTTPFileInfo ReadWriteBufferFromHTTPBase<UpdatableSessionPtr>::parseFileInfo(const Poco::Net::HTTPResponse & response, size_t requested_range_begin)
{
    HTTPFileInfo res;

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

}

SessionFactory::SessionFactory(const ConnectionTimeouts & timeouts_, Poco::Net::HTTPClientSession::ProxyConfig proxy_config_)
    : timeouts(timeouts_), proxy_config(proxy_config_) {}

SessionFactory::SessionType SessionFactory::buildNewSession(const Poco::URI & uri)
{
    return makeHTTPSession(uri, timeouts, proxy_config);
}

ReadWriteBufferFromHTTP::ReadWriteBufferFromHTTP(
    Poco::URI uri_,
    const std::string & method_,
    OutStreamCallback out_stream_callback_,
    const ConnectionTimeouts & timeouts,
    const Poco::Net::HTTPBasicCredentials & credentials_,
    const UInt64 max_redirects,
    size_t buffer_size_,
    const ReadSettings & settings_,
    const HTTPHeaderEntries & http_header_entries_,
    const RemoteHostFilter * remote_host_filter_,
    bool delay_initialization_,
    bool use_external_buffer_,
    bool skip_not_found_url_,
    std::optional<HTTPFileInfo> file_info_,
    Poco::Net::HTTPClientSession::ProxyConfig proxy_config_)
    : Parent(
        std::make_shared<SessionType>(uri_, max_redirects, std::make_shared<SessionFactory>(timeouts, proxy_config_)),
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
        file_info_,
        proxy_config_) {}


PooledSessionFactory::PooledSessionFactory(
    const ConnectionTimeouts & timeouts_, size_t per_endpoint_pool_size_)
    : timeouts(timeouts_)
    , per_endpoint_pool_size(per_endpoint_pool_size_) {}

PooledSessionFactory::SessionType PooledSessionFactory::buildNewSession(const Poco::URI & uri)
{
    return makePooledHTTPSession(uri, timeouts, per_endpoint_pool_size);
}


PooledReadWriteBufferFromHTTP::PooledReadWriteBufferFromHTTP(
    Poco::URI uri_,
    const std::string & method_,
    OutStreamCallback out_stream_callback_,
    const ConnectionTimeouts & timeouts_,
    const Poco::Net::HTTPBasicCredentials & credentials_,
    size_t buffer_size_,
    const UInt64 max_redirects,
    size_t max_connections_per_endpoint)
    : Parent(
        std::make_shared<SessionType>(uri_, max_redirects, std::make_shared<PooledSessionFactory>(timeouts_, max_connections_per_endpoint)),
        uri_,
        credentials_,
        method_,
        out_stream_callback_,
        buffer_size_) {}


template class UpdatableSession<SessionFactory>;
template class UpdatableSession<PooledSessionFactory>;
template class detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<SessionFactory>>>;
template class detail::ReadWriteBufferFromHTTPBase<std::shared_ptr<UpdatableSession<PooledSessionFactory>>>;

}
