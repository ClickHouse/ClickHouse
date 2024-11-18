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
#include <Common/RemoteHostFilter.h>
#include "config.h"
#include <Common/config_version.h>

#include <filesystem>

namespace DB
{

class ReadWriteBufferFromHTTP : public SeekableReadBuffer, public WithFileName, public WithFileSize
{
public:
    /// Information from HTTP response header.
    struct HTTPFileInfo
    {
        // nullopt if the server doesn't report it.
        std::optional<size_t> file_size;
        std::optional<time_t> last_modified;
        bool seekable = false;
    };

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

    const bool use_external_buffer;
    const bool http_skip_not_found_url;
    bool has_not_found_url = false;

    std::function<void(std::ostream &)> out_stream_callback;

    Poco::URI current_uri;
    size_t redirects = 0;

    std::string content_encoding;
    std::unique_ptr<ReadBuffer> impl;

    std::vector<Poco::Net::HTTPCookie> cookies;

    std::map<String, String> response_headers;

    HTTPHeaderEntries http_header_entries;
    std::function<void(size_t)> next_callback;

    size_t offset_from_begin_pos = 0;
    HTTPRange read_range;
    std::optional<HTTPFileInfo> file_info;

    LoggerPtr log;

    bool withPartialContent() const;

    void prepareRequest(Poco::Net::HTTPRequest & request, std::optional<HTTPRange> range) const;

    void doWithRetries(std::function<void()> && callable, std::function<void()> on_retry = nullptr, bool mute_logging = false) const;

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

public:
    using NextCallback = std::function<void(size_t)>;
    using OutStreamCallback = std::function<void(std::ostream &)>;

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
        OutStreamCallback out_stream_callback_,
        bool use_external_buffer_,
        bool http_skip_not_found_url_,
        HTTPHeaderEntries http_header_entries_,
        bool delay_initialization,
        std::optional<HTTPFileInfo> file_info_);

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
};

using ReadWriteBufferFromHTTPPtr = std::unique_ptr<ReadWriteBufferFromHTTP>;

class BuilderRWBufferFromHTTP
{
    Poco::URI uri;
    std::string method = Poco::Net::HTTPRequest::HTTP_GET;
    HTTPConnectionGroupType connection_group = HTTPConnectionGroupType::HTTP;
    ProxyConfiguration proxy_config{};
    ReadSettings read_settings{};
    ConnectionTimeouts timeouts{};
    const RemoteHostFilter * remote_host_filter = nullptr;
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t max_redirects = 0;
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = nullptr;
    bool use_external_buffer = false;
    bool http_skip_not_found_url = false;
    HTTPHeaderEntries http_header_entries{};
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
    setterMember(withProxy, proxy_config)
    setterMember(withSettings, read_settings)
    setterMember(withTimeouts, timeouts)
    setterMember(withHostFilter, remote_host_filter)
    setterMember(withBufSize, buffer_size)
    setterMember(withRedirects, max_redirects)
    setterMember(withOutCallback, out_stream_callback)
    setterMember(withHeaders, http_header_entries)
    setterMember(withExternalBuf, use_external_buffer)
    setterMember(withDelayInit, delay_initialization)
    setterMember(withSkipNotFound, http_skip_not_found_url)
#undef setterMember
/// NOLINTEND(bugprone-macro-parentheses)

    ReadWriteBufferFromHTTPPtr create(const Poco::Net::HTTPBasicCredentials & credentials_)
    {
        return std::make_unique<ReadWriteBufferFromHTTP>(
            connection_group,
            uri,
            method,
            proxy_config,
            read_settings,
            timeouts,
            credentials_,
            remote_host_filter,
            buffer_size,
            max_redirects,
            out_stream_callback,
            use_external_buffer,
            http_skip_not_found_url,
            http_header_entries,
            delay_initialization,
            /*file_info_=*/ std::nullopt);
    }
};

}
