#include "ReadWriteBufferFromHTTP.h"

#include <IO/HTTPCommon.h>
#include <Common/NetException.h>
#include <Poco/Net/NetException.h>


namespace ProfileEvents
{
    extern const Event ReadBufferSeekCancelConnection;
    extern const Event ReadWriteBufferFromHTTPRequestsSent;
    extern const Event ReadWriteBufferFromHTTPBytes;
}


namespace
{

bool isRetriableError(const Poco::Net::HTTPResponse::HTTPStatus http_status) noexcept
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

Poco::URI getUriAfterRedirect(const Poco::URI & prev_uri, Poco::Net::HTTPResponse & response)
{
    chassert(DB::isRedirect(response.getStatus()));

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

class ReadBufferFromSessionResponse : public DB::ReadBufferFromIStream
{
private:
    DB::HTTPSessionPtr session;

public:
    ReadBufferFromSessionResponse(DB::HTTPSessionPtr && session_, std::istream & rstr, size_t size)
    : ReadBufferFromIStream(rstr, size)
    , session(std::move(session_))
    {
    }
};

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
}

std::unique_ptr<ReadBuffer> ReadWriteBufferFromHTTP::CallResult::transformToReadBuffer(size_t buf_size) &&
{
    chassert(session);
    return std::make_unique<ReadBufferFromSessionResponse>(std::move(session), *response_stream, buf_size);
}

bool ReadWriteBufferFromHTTP::withPartialContent() const
{
    /**
     * Add range header if we have some passed range
     * or if we want to retry GET request on purpose.
     */
    return read_range.begin || read_range.end || getOffset() > 0;
}

size_t ReadWriteBufferFromHTTP::getOffset() const
{
    return read_range.begin.value_or(0) + offset_from_begin_pos;
}

void ReadWriteBufferFromHTTP::prepareRequest(Poco::Net::HTTPRequest & request, std::optional<HTTPRange> range) const
{
    request.setHost(current_uri.getHost());

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
        request.set("Range", range_header_value);
    }

    if (!credentials.getUsername().empty())
        credentials.authenticate(request);
}

std::optional<size_t> ReadWriteBufferFromHTTP::tryGetFileSize()
{
    if (!file_info)
    {
        try
        {
            file_info = getFileInfo();
        }
        catch (const HTTPException &)
        {
            return std::nullopt;
        }
        catch (const NetException &)
        {
            return std::nullopt;
        }
        catch (const Poco::Net::NetException &)
        {
            return std::nullopt;
        }
        catch (const Poco::IOException &)
        {
            return std::nullopt;
        }
    }

    return file_info->file_size;
}

bool ReadWriteBufferFromHTTP::supportsReadAt()
{
    if (!file_info)
        file_info = getFileInfo();
    return method == Poco::Net::HTTPRequest::HTTP_GET && file_info->seekable;
}

bool ReadWriteBufferFromHTTP::checkIfActuallySeekable()
{
    if (!file_info)
        file_info = getFileInfo();
    return file_info->seekable;
}

String ReadWriteBufferFromHTTP::getFileName() const
{
    return initial_uri.toString();
}

void ReadWriteBufferFromHTTP::getHeadResponse(Poco::Net::HTTPResponse & response)
{
    doWithRetries(
        [&] ()
        {
            callWithRedirects(response, Poco::Net::HTTPRequest::HTTP_HEAD, {});
        },
        /*on_retry=*/ nullptr,
        /*mute_logging=*/ true);
}

ReadWriteBufferFromHTTP::ReadWriteBufferFromHTTP(
    const HTTPConnectionGroupType & connection_group_,
    const Poco::URI & uri_,
    const std::string & method_,
    ProxyConfiguration proxy_config_,
    ReadSettings read_settings_,
    ConnectionTimeouts timeouts_,
    const Poco::Net::HTTPBasicCredentials & credentials_,
    const RemoteHostFilter * remote_host_filter_,
    size_t buffer_size_,
    size_t max_redirects_,
    OutStreamCallback out_stream_callback_,
    bool use_external_buffer_,
    bool http_skip_not_found_url_,
    HTTPHeaderEntries http_header_entries_,
    bool delay_initialization,
    std::optional<HTTPFileInfo> file_info_)
    : SeekableReadBuffer(nullptr, 0)
    , connection_group(connection_group_)
    , initial_uri(uri_)
    , method(!method_.empty() ? method_ : out_stream_callback_ ? Poco::Net::HTTPRequest::HTTP_POST : Poco::Net::HTTPRequest::HTTP_GET)
    , proxy_config(std::move(proxy_config_))
    , read_settings(std::move(read_settings_))
    , timeouts(std::move(timeouts_))
    , credentials(credentials_)
    , remote_host_filter(remote_host_filter_)
    , buffer_size(buffer_size_)
    , max_redirects(max_redirects_)
    , use_external_buffer(use_external_buffer_)
    , http_skip_not_found_url(http_skip_not_found_url_)
    , out_stream_callback(std::move(out_stream_callback_))
    , redirects(0)
    , http_header_entries {std::move(http_header_entries_)}
    , file_info(file_info_)
    , log(getLogger("ReadWriteBufferFromHTTP"))
{
    current_uri = initial_uri;

    if (current_uri.getPath().empty())
        current_uri.setPath("/");

    if (read_settings.http_max_tries <= 0 || read_settings.http_retry_initial_backoff_ms <= 0
        || read_settings.http_retry_initial_backoff_ms >= read_settings.http_retry_max_backoff_ms)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid setting for http backoff, "
            "must be http_max_tries >= 1 (current is {}) and "
            "0 < http_retry_initial_backoff_ms < settings.http_retry_max_backoff_ms (now 0 < {} < {})",
            read_settings.http_max_tries,
            read_settings.http_retry_initial_backoff_ms,
            read_settings.http_retry_max_backoff_ms);

    // Configure User-Agent if it not already set.
    const std::string user_agent = "User-Agent";
    auto iter = std::find_if(http_header_entries.begin(), http_header_entries.end(),
        [&user_agent] (const HTTPHeaderEntry & entry) { return entry.name == user_agent; });

    if (iter == http_header_entries.end())
    {
        http_header_entries.emplace_back(user_agent, fmt::format("ClickHouse/{}{}", VERSION_STRING, VERSION_OFFICIAL));
    }

    if (!delay_initialization && use_external_buffer)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid setting for ReadWriteBufferFromHTTP"
            "delay_initialization is false and use_external_buffer it true.");

    if (!delay_initialization)
    {
        next();
    }
}

ReadWriteBufferFromHTTP::CallResult ReadWriteBufferFromHTTP::callImpl(
    Poco::Net::HTTPResponse & response, const std::string & method_, const std::optional<HTTPRange> & range, bool allow_redirects) const
{
    if (remote_host_filter)
        remote_host_filter->checkURL(current_uri);

    Poco::Net::HTTPRequest request(method_, current_uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
    prepareRequest(request, range);

    auto session = makeHTTPSession(connection_group, current_uri, timeouts, proxy_config);

    ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPRequestsSent);

    auto & stream_out = session->sendRequest(request);
    if (out_stream_callback)
        out_stream_callback(stream_out);

    auto & resp_stream = session->receiveResponse(response);

    assertResponseIsOk(current_uri.toString(), response, resp_stream, allow_redirects);

    return ReadWriteBufferFromHTTP::CallResult(std::move(session), resp_stream);
}

ReadWriteBufferFromHTTP::CallResult ReadWriteBufferFromHTTP::callWithRedirects(
    Poco::Net::HTTPResponse & response, const String & method_, const std::optional<HTTPRange> & range)
{
    auto result = callImpl(response, method_, range, true);

    while (isRedirect(response.getStatus()))
    {
        Poco::URI uri_redirect = getUriAfterRedirect(current_uri, response);
        ++redirects;
        if (redirects > max_redirects)
            throw Exception(
                ErrorCodes::TOO_MANY_REDIRECTS,
                "Too many redirects while trying to access {}."
                " You can {} redirects by changing the setting 'max_http_get_redirects'."
                " Example: `SET max_http_get_redirects = 10`."
                " Redirects are restricted to prevent possible attack when a malicious server redirects to an internal resource, bypassing the authentication or firewall.",
                initial_uri.toString(), max_redirects ? "increase the allowed maximum number of" : "allow");

        current_uri = uri_redirect;
        result = callImpl(response, method_, range, true);
    }

    return result;
}


void ReadWriteBufferFromHTTP::doWithRetries(std::function<void()> && callable,
                                            std::function<void()> on_retry,
                                            bool mute_logging) const
{
    [[maybe_unused]] auto milliseconds_to_wait = read_settings.http_retry_initial_backoff_ms;

    bool is_retriable = true;
    std::exception_ptr exception = nullptr;

    for (size_t attempt = 1; attempt <= read_settings.http_max_tries; ++attempt)
    {
        [[maybe_unused]] bool last_attempt = attempt + 1 > read_settings.http_max_tries;

        String error_message;

        try
        {
            callable();
            return;
        }
        catch (Poco::Net::NetException & e)
        {
            error_message = e.displayText();
            exception = std::current_exception();
        }
        catch (NetException & e)
        {
            error_message = e.displayText();
            exception = std::current_exception();
        }
        catch (HTTPException & e)
        {
            if (!isRetriableError(e.getHTTPStatus()))
                is_retriable = false;

            error_message = e.displayText();
            exception = std::current_exception();
        }
        catch (Exception & e)
        {
            is_retriable = false;

            error_message = e.displayText();
            exception = std::current_exception();
        }
        catch (Poco::Exception & e)
        {
            if (e.code() == POCO_EMFILE)
                is_retriable = false;

            error_message = e.displayText();
            exception = std::current_exception();
        }

        chassert(exception);

        if (last_attempt || !is_retriable)
        {
            if (!mute_logging)
                LOG_DEBUG(log,
                          "Failed to make request to '{}'{}. "
                          "Error: '{}'. "
                          "Failed at try {}/{}.",
                          initial_uri.toString(), current_uri == initial_uri ? String() : fmt::format(" redirect to '{}'", current_uri.toString()),
                          error_message,
                          attempt, read_settings.http_max_tries);

            std::rethrow_exception(exception);
        }
        else
        {
            if (on_retry)
                on_retry();

            if (!mute_logging)
                LOG_TRACE(log,
                         "Failed to make request to '{}'{}. "
                         "Error: {}. "
                         "Failed at try {}/{}. "
                         "Will retry with current backoff wait is {}/{} ms.",
                         initial_uri.toString(), current_uri == initial_uri ? String() : fmt::format(" redirect to '{}'", current_uri.toString()),
                         error_message,
                         attempt + 1, read_settings.http_max_tries,
                         milliseconds_to_wait, read_settings.http_retry_max_backoff_ms);

            sleepForMilliseconds(milliseconds_to_wait);
            milliseconds_to_wait = std::min(milliseconds_to_wait * 2, read_settings.http_retry_max_backoff_ms);
        }
    }
}


std::unique_ptr<ReadBuffer> ReadWriteBufferFromHTTP::initialize()
{
    Poco::Net::HTTPResponse response;

    std::optional<HTTPRange> range;
    if (withPartialContent())
        range = HTTPRange{getOffset(), read_range.end};

    auto result = callWithRedirects(response, method, range);

    if (range.has_value() && response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT)
    {
        /// Having `200 OK` instead of `206 Partial Content` is acceptable in case we retried with range.begin == 0.
        if (getOffset() != 0)
        {
            /// Retry 200OK
            if (response.getStatus() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
            {
                String reason = fmt::format(
                    "Cannot read with range: [{}, {}] (response status: {}, reason: {}), will retry",
                    *read_range.begin, read_range.end ? toString(*read_range.end) : "-",
                    toString(response.getStatus()), response.getReason());

                /// it is retriable error
                throw HTTPException(
                    ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                    current_uri.toString(),
                    Poco::Net::HTTPResponse::HTTP_REQUESTED_RANGE_NOT_SATISFIABLE,
                    reason);
            }
            throw Exception(
                ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                "Cannot read with range: [{}, {}] (response status: {}, reason: {})",
                *read_range.begin,
                read_range.end ? toString(*read_range.end) : "-",
                toString(response.getStatus()),
                response.getReason());
        }
        if (read_range.end)
        {
            /// We could have range.begin == 0 and range.end != 0 in case of DiskWeb and failing to read with partial content
            /// will affect only performance, so a warning is enough.
            LOG_WARNING(log, "Unable to read with range header: [{}, {}]", read_range.begin.value_or(0), *read_range.end);
        }
    }

    response.getCookies(cookies);
    response.getHeaders(response_headers);
    content_encoding = response.get("Content-Encoding", "");

    // Remember file size. It'll be used to report eof in next nextImpl() call.
    if (!read_range.end && response.hasContentLength())
        file_info = parseFileInfo(response, range.has_value() ? getOffset() : 0);

    return std::move(result).transformToReadBuffer(use_external_buffer ? 0 : buffer_size);
}

bool ReadWriteBufferFromHTTP::nextImpl()
{
    if (next_callback)
        next_callback(count());

    bool next_result = false;

    doWithRetries(
        /*callable=*/ [&] ()
        {
            if (!impl)
            {
                try
                {
                    impl = initialize();
                }
                catch (HTTPException & e)
                {
                    if (http_skip_not_found_url && e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND)
                    {
                        next_result = false;
                        has_not_found_url = true;
                        return;
                    }

                    throw;
                }

                if (use_external_buffer)
                {
                    impl->set(internal_buffer.begin(), internal_buffer.size());
                }
                else
                {
                    BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());
                }
            }

            if (use_external_buffer)
            {
                impl->set(internal_buffer.begin(), internal_buffer.size());
            }
            else
            {
                impl->position() = position();
            }

            next_result = impl->next();

            BufferBase::set(impl->buffer().begin(), impl->buffer().size(), impl->offset());

            offset_from_begin_pos += working_buffer.size();

            ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPBytes, working_buffer.size());
        },
        /*on_retry=*/ [&] ()
        {
            impl.reset();
        });

    return next_result;
}

size_t ReadWriteBufferFromHTTP::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const
{
    /// Caller must have checked supportsReadAt().
    /// This ensures we've sent at least one HTTP request and populated current_uri.
    chassert(file_info && file_info->seekable);

    size_t initial_n = n;
    size_t total_bytes_copied = 0;
    size_t bytes_copied = 0;
    bool is_canceled = false;

    doWithRetries(
        /*callable=*/ [&] ()
        {
            auto range = HTTPRange{offset, offset + n - 1};

            Poco::Net::HTTPResponse response;
            auto result = callImpl(response, method, range, false);

            if (response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_PARTIAL_CONTENT &&
                (offset != 0 || offset + n < *file_info->file_size))
            {
                String reason = fmt::format(
                    "When reading with readBigAt {}."
                    "Cannot read with range: [{}, {}] (response status: {}, reason: {}), will retry",
                    initial_uri.toString(),
                    *range.begin, *range.end,
                    toString(response.getStatus()), response.getReason());

                throw HTTPException(
                    ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE,
                    current_uri.toString(),
                    Poco::Net::HTTPResponse::HTTP_REQUESTED_RANGE_NOT_SATISFIABLE,
                    reason);
            }

            copyFromIStreamWithProgressCallback(*result.response_stream, to, n, progress_callback, &bytes_copied, &is_canceled);

            ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPBytes, bytes_copied);

            offset += bytes_copied;
            total_bytes_copied += bytes_copied;
            to += bytes_copied;
            n -= bytes_copied;
            bytes_copied = 0;
        },
        /*on_retry=*/ [&] ()
        {
            ProfileEvents::increment(ProfileEvents::ReadWriteBufferFromHTTPBytes, bytes_copied);

            offset += bytes_copied;
            total_bytes_copied += bytes_copied;
            to += bytes_copied;
            n -= bytes_copied;
            bytes_copied = 0;
        });

    chassert(total_bytes_copied == initial_n || is_canceled);
    return total_bytes_copied;
}

off_t ReadWriteBufferFromHTTP::getPosition()
{
    return getOffset() - available();
}

off_t ReadWriteBufferFromHTTP::seek(off_t offset_, int whence)
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
        chassert(pos >= working_buffer.begin());
        chassert(pos < working_buffer.end());

        return getPosition();
    }

    if (impl)
    {
        auto position = getPosition();
        if (offset_ >= position)
        {
            size_t diff = offset_ - position;
            if (diff < read_settings.remote_read_min_bytes_for_seek)
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


void ReadWriteBufferFromHTTP::setReadUntilPosition(size_t until)
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

void ReadWriteBufferFromHTTP::setReadUntilEnd()
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

bool ReadWriteBufferFromHTTP::supportsRightBoundedReads() const { return true; }

bool ReadWriteBufferFromHTTP::atEndOfRequestedRangeGuess()
{
    if (!impl)
        return true;
    if (read_range.end)
        return getPosition() > static_cast<off_t>(*read_range.end);
    if (file_info && file_info->file_size)
        return getPosition() >= static_cast<off_t>(*file_info->file_size);
    return false;
}

std::string ReadWriteBufferFromHTTP::getResponseCookie(const std::string & name, const std::string & def) const
{
    for (const auto & cookie : cookies)
        if (cookie.getName() == name)
            return cookie.getValue();
    return def;
}

Map ReadWriteBufferFromHTTP::getResponseHeaders() const
{
    Map map;
    for (const auto & header : response_headers)
    {
        Tuple elem;
        elem.emplace_back(header.first);
        elem.emplace_back(header.second);
        map.emplace_back(elem);
    }
    return map;
}

void ReadWriteBufferFromHTTP::setNextCallback(NextCallback next_callback_)
{
    next_callback = next_callback_;
    /// Some data maybe already read
    next_callback(count());
}

const std::string & ReadWriteBufferFromHTTP::getCompressionMethod() const
{
    return content_encoding;
}

std::optional<time_t> ReadWriteBufferFromHTTP::tryGetLastModificationTime()
{
    if (!file_info)
    {
        try
        {
            file_info = getFileInfo();
        }
        catch (const HTTPException &)
        {
            return std::nullopt;
        }
        catch (const NetException &)
        {
            return std::nullopt;
        }
        catch (const Poco::Net::NetException &)
        {
            return std::nullopt;
        }
        catch (const Poco::IOException &)
        {
            return std::nullopt;
        }
    }

    return file_info->last_modified;
}

ReadWriteBufferFromHTTP::HTTPFileInfo ReadWriteBufferFromHTTP::getFileInfo()
{
    /// May be disabled in case the user knows in advance that the server doesn't support HEAD requests.
    /// Allows to avoid making unnecessary requests in such cases.
    if (!read_settings.http_make_head_request)
        return HTTPFileInfo{};

    Poco::Net::HTTPResponse response;
    try
    {
        getHeadResponse(response);
    }
    catch (const HTTPException & e)
    {
        /// Maybe the web server doesn't support HEAD requests.
        /// E.g. webhdfs reports status 400.
        /// We should proceed in hopes that the actual GET request will succeed.
        /// (Unless the error in transient. Don't want to nondeterministically sometimes
        /// fall back to slow whole-file reads when HEAD is actually supported; that sounds
        /// like a nightmare to debug.)
        if (e.getHTTPStatus() >= 400 && e.getHTTPStatus() <= 499 &&
            e.getHTTPStatus() != Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS &&
            e.getHTTPStatus() != Poco::Net::HTTPResponse::HTTP_REQUEST_TIMEOUT &&
            e.getHTTPStatus() != Poco::Net::HTTPResponse::HTTP_MISDIRECTED_REQUEST)
        {
            return HTTPFileInfo{};
        }

        throw;
    }

    return parseFileInfo(response, 0);
}

ReadWriteBufferFromHTTP::HTTPFileInfo ReadWriteBufferFromHTTP::parseFileInfo(const Poco::Net::HTTPResponse & response, size_t requested_range_begin)
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
