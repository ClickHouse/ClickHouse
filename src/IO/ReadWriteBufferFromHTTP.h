#pragma once

#include <functional>
#include <Core/Field.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/IReadBufferMetadataProvider.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WithFileName.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <base/types.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/URIStreamFactory.h>
#include <Common/RemoteHostFilter.h>
#include <Common/config_version.h>

#include <condition_variable>
#include <filesystem>
#include <mutex>

namespace DB
{

class ReadWriteBufferFromHTTP : public SeekableReadBuffer, public WithFileName, public WithFileSize, public IReadBufferMetadataProvider
{
    friend class BuilderRWBufferFromHTTP;
public:
    /// Information from HTTP response header.
    struct HTTPFileInfo
    {
        // nullopt if the server doesn't report it.
        std::optional<size_t> file_size;
        std::optional<time_t> last_modified;
        bool seekable = false;
    };

    using OutStreamCallback = std::function<void(std::ostream &)>;
    using NextCallback = std::function<void(size_t)>;
    using RedirectCallback = std::function<void(const Poco::URI &, const Poco::URI &)>;

    /** A one-shot flag which the code that reads from the buffer sets when it does not need the data
      * anymore, for example when the query pipeline it reads for is being torn down. The buffer only
      * uses it to stop retrying: unlike a predicate it can be waited on, which makes the backoff
      * between the retry attempts interruptible, so a cancellation does not have to be waited out.
      */
    class Cancellation
    {
    public:
        /// Called from a cancellation handler, which is not allowed to throw. `softly` means the
        /// cancellation is one after which the query must still succeed with what it has already
        /// read (see StorageURLSource::cancel): the code in between the requests may then report
        /// the interruption with a cancellation error of its own, which the owner of this flag
        /// discards. After a hard teardown - the query is killed, the pipeline has already failed
        /// elsewhere, or the client has disconnected - a synthesized error could mask the failure
        /// that really happened, so nothing is allowed to invent one, see
        /// StorageURLSource::getFirstAvailableURIAndReadBuffer.
        void cancel(bool softly) noexcept
        {
            {
                std::lock_guard lock(mutex);
                /// A hard cancellation is final even after a soft one: ExecutingGraph::cancel
                /// upgrades PartialResult to the reason of a later hard cancellation and delivers
                /// the upgrade here, after which the query fails and nothing may report the
                /// interruption as a cancellation anymore. The opposite downgrade does not exist,
                /// so the read stays soft only while every reason so far has been soft.
                cancelled_softly = cancelled ? (cancelled_softly && softly) : softly;
                cancelled = true;
            }
            changed.notify_all();
        }

        /// Returns true if the read has been cancelled, either already or before the time has passed.
        bool waitForCancellation(std::chrono::milliseconds duration)
        {
            std::unique_lock lock(mutex);
            return changed.wait_for(lock, duration, [this] { return cancelled; });
        }

        /// A non-blocking check for the code in between the requests, which must not go on making
        /// more of them after the read has been cancelled.
        bool isCancelled()
        {
            std::lock_guard lock(mutex);
            return cancelled;
        }

        /// Whether the read has been cancelled softly, see cancel.
        bool isCancelledSoftly()
        {
            std::lock_guard lock(mutex);
            return cancelled_softly;
        }

    private:
        std::mutex mutex;
        std::condition_variable changed;
        bool cancelled = false;
        bool cancelled_softly = false;
    };

    using CancellationPtr = std::shared_ptr<Cancellation>;

    const Poco::URI & getCurrentURI() const { return current_uri; }

private:
    /// Byte range, including right bound [begin, end].
    struct HTTPRange
    {
        std::optional<size_t> begin;
        std::optional<size_t> end;
    };

    struct CallResult
    {
        HTTPSessionPtr session;
        std::istream * response_stream = nullptr;

        CallResult(HTTPSessionPtr && session_, std::istream & response_stream_)
            : session(session_)
            , response_stream(&response_stream_)
        {}
        CallResult(CallResult &&) = default;
        CallResult & operator= (CallResult &&) = default;

        std::unique_ptr<ReadBuffer> transformToReadBuffer(size_t buf_size) &&;
    };

    const HTTPConnectionGroupType connection_group;
    const Poco::URI initial_uri;
    const std::string method;
    const ProxyConfiguration proxy_config;
    const ReadSettings read_settings;
    const ConnectionTimeouts timeouts;

    const Poco::Net::HTTPBasicCredentials & credentials;
    const RemoteHostFilter * remote_host_filter;

    const size_t buffer_size;
    const size_t max_redirects;
    const bool enable_url_encoding;

    const bool use_external_buffer;
    const bool http_skip_not_found_url;
    bool has_not_found_url = false;

    OutStreamCallback out_stream_callback;
    RedirectCallback redirect_callback;

    Poco::URI current_uri;
    size_t redirects = 0;

    std::string content_encoding;
    std::unique_ptr<ReadBuffer> impl;

    std::vector<Poco::Net::HTTPCookie> cookies; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    std::map<String, String> response_headers; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    HTTPHeaderEntries http_header_entries;
    std::function<void(size_t)> next_callback;

    size_t offset_from_begin_pos = 0;
    HTTPRange read_range;
    std::optional<HTTPFileInfo> file_info;

    LoggerPtr log;

    /// Set by the code that reads from this buffer when it does not need the data anymore. Retrying an
    /// HTTP request stops as soon as it is set, see doWithRetries.
    CancellationPtr cancellation;

    bool withPartialContent() const;

    void prepareRequest(Poco::Net::HTTPRequest & request, std::optional<HTTPRange> range) const;

    void doWithRetries(std::function<void()> && callable, std::function<void()> on_retry = nullptr, bool mute_logging = false) const;

    /// Waits before the next retry attempt. Returns true if the read has been cancelled while waiting.
    bool waitBeforeRetry(size_t milliseconds) const;

    /// Whether the code that reads from this buffer has cancelled the read, see doWithRetries. The
    /// helpers which swallow the errors of the requests they make must not swallow the error of a
    /// request interrupted by a cancellation - nothing may go on requesting after it.
    bool isReadCancelled() const;

    CallResult  callImpl(
        Poco::Net::HTTPResponse & response,
        const std::string & method_,
        const std::optional<HTTPRange> & range,
        bool allow_redirects) const;

    CallResult  callWithRedirects(
        Poco::Net::HTTPResponse & response,
        const String & method_,
        const std::optional<HTTPRange> & range);

    std::unique_ptr<ReadBuffer> initialize();

    std::optional<size_t> tryGetFileSize() override;

    bool supportsReadAt() override;

    bool checkIfActuallySeekable() override;

    String getFileName() const override;

    void getHeadResponse(Poco::Net::HTTPResponse & response);

    void setupExternalBuffer();

    size_t getOffset() const;

    // If true, if we destroy impl now, no work was wasted. Just for metrics.
    bool atEndOfRequestedRangeGuess();

    ReadWriteBufferFromHTTP(
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
        bool enable_url_encoding_,
        OutStreamCallback out_stream_callback_,
        CancellationPtr cancellation_,
        bool use_external_buffer_,
        bool http_skip_not_found_url_,
        HTTPHeaderEntries http_header_entries_,
        RedirectCallback redirect_callback_,
        bool delay_initialization,
        std::optional<HTTPFileInfo> file_info_);

public:
    bool nextImpl() override;

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const override;

    off_t seek(off_t offset_, int whence) override;

    void setReadUntilPosition(size_t until) override;

    void setReadUntilEnd() override;

    bool supportsRightBoundedReads() const override;

    off_t getPosition() override;

    std::string getResponseCookie(const std::string & name, const std::string & def) const;

    /// Set function to call on each nextImpl, useful when you need to track
    /// progress.
    /// NOTE: parameter on each call is not incremental -- it's all bytes count
    /// passed through the buffer
    void setNextCallback(NextCallback next_callback_);

    const std::string & getCompressionMethod() const;

    std::optional<time_t> tryGetLastModificationTime();

    bool hasNotFoundURL() const { return has_not_found_url; }

    HTTPFileInfo getFileInfo();
    static HTTPFileInfo parseFileInfo(const Poco::Net::HTTPResponse & response, size_t requested_range_begin);

    Map getResponseHeaders() const;
    std::optional<Field> getMetadata(const String & name) const override;
};

using ReadWriteBufferFromHTTPPtr = std::unique_ptr<ReadWriteBufferFromHTTP>;

class BuilderRWBufferFromHTTP
{
    Poco::URI uri;
    std::string method = Poco::Net::HTTPRequest::HTTP_GET;
    HTTPConnectionGroupType connection_group = HTTPConnectionGroupType::HTTP;
    bool bypass_proxy = false;
    ReadSettings read_settings{};
    ConnectionTimeouts timeouts{};
    const RemoteHostFilter * remote_host_filter = nullptr;
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t max_redirects = 0;
    bool enable_url_encoding = false;
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = nullptr;
    ReadWriteBufferFromHTTP::RedirectCallback redirect_callback = nullptr;
    bool use_external_buffer = false;
    bool http_skip_not_found_url = false;
    HTTPHeaderEntries http_header_entries{};
    ReadWriteBufferFromHTTP::CancellationPtr cancellation;
    bool delay_initialization = true;

public:
    explicit BuilderRWBufferFromHTTP(Poco::URI uri_)
        : uri(uri_)
    {}

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define setterMember(name, member) \
    BuilderRWBufferFromHTTP & name(decltype(BuilderRWBufferFromHTTP::member) arg_##member) \
    { \
        member = std::move(arg_##member); \
        return *this; \
    }

    setterMember(withConnectionGroup, connection_group)
    setterMember(withMethod, method)
    setterMember(withBypassProxy, bypass_proxy)
    setterMember(withSettings, read_settings)
    setterMember(withTimeouts, timeouts)
    setterMember(withHostFilter, remote_host_filter)
    setterMember(withBufSize, buffer_size)
    setterMember(withRedirects, max_redirects)
    setterMember(withEnableUrlEncoding, enable_url_encoding)
    setterMember(withOutCallback, out_stream_callback)
    setterMember(withRedirectCallback, redirect_callback)
    setterMember(withHeaders, http_header_entries)
    setterMember(withCancellation, cancellation)
    setterMember(withExternalBuf, use_external_buffer)
    setterMember(withDelayInit, delay_initialization)
    setterMember(withSkipNotFound, http_skip_not_found_url)
#undef setterMember
/// NOLINTEND(bugprone-macro-parentheses)

    ReadWriteBufferFromHTTPPtr create(const Poco::Net::HTTPBasicCredentials & credentials_);
};

/// Fills `credentials` from the userinfo component of `uri` (e.g. `http://user:pass@host`).
void setCredentialsFromURL(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & uri);

}
