#pragma once

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponseBase.h>

#include <IO/WriteBufferFromPocoSocket.h>

#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class HTTP1ServerResponse;

/// Postpone sending HTTP header until first data is flushed. This is needed in HTTP servers
///  to change some HTTP headers (e.g. response code) before any data is sent to the client.
///
/// Also this class write and flush special X-ClickHouse-Progress HTTP headers
///  if no data was sent at the time of progress notification.
/// This allows to implement progress bar in HTTP clients.
class WriteBufferFromHTTP1ServerResponse final : public WriteBufferFromHTTPServerResponseBase
{
public:
    static constexpr std::string_view EXCEPTION_MARKER = "__exception__";
    static constexpr size_t EXCEPTION_TAG_LENGTH = 16;
    static constexpr size_t MAX_EXCEPTION_SIZE= 16 * 1024; // 16K

    WriteBufferFromHTTP1ServerResponse(
        HTTP1ServerResponse & response,
        bool is_http_method_head,
        const ProfileEvents::Event & write_event = ProfileEvents::end(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void sendBufferAndFinalize(const char * ptr, size_t size) override;

    /// Writes progress in repeating HTTP headers.
    void onProgress(const Progress & progress, ContextPtr context) override;

    /// Turn CORS on or off.
    /// The setting has any effect only if HTTP headers haven't been sent yet.
    void addHeaderCORS(bool enable_cors)
    {
        add_cors_header = enable_cors;
    }

    /// Send progress
    void setSendProgress(bool send_progress_) { send_progress = send_progress_; }

    /// Don't send HTTP headers with progress more frequently.
    void setSendProgressInterval(size_t send_progress_interval_ms_)
    {
        send_progress_interval_ms = send_progress_interval_ms_;
    }

    /// Content-Encoding header will be set on first data package
    void setCompressionMethodHeader(const CompressionMethod & compression_method_)
    {
        compression_method = compression_method_;
    }

    bool isChunked() const;

    bool isFixedLength() const;

    void setExceptionCode(int code) override;

    bool cancelWithException(int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept override;

private:
    /// Send at least HTTP headers if no data has been sent yet.
    /// Use after the data has possibly been sent and no error happened (and thus you do not plan
    /// to change response HTTP code.
    /// This method is idempotent.
    void finalizeImpl() override;

    void cancelImpl() noexcept override;

    /// Must be called under locked mutex.
    /// This method send headers, if this was not done already,
    ///  but not finish them with \r\n, allowing to send more headers subsequently.
    void startSendHeaders();

    /// Used to write the header X-ClickHouse-Progress / X-ClickHouse-Summary
    void writeHeaderProgressImpl(const char * header_name, Progress::DisplayMode mode);
    /// Used to write the header X-ClickHouse-Progress
    void writeHeaderProgress();
    /// Used to write the header X-ClickHouse-Summary
    void writeHeaderSummary();
    /// Use to write the header X-ClickHouse-Exception-Code even when progress has been sent
    void writeExceptionCode();

    /// This method finish headers with \r\n, allowing to start to send body.
    void finishSendHeaders(bool send_exception_tag = true);

    void nextImpl() override;

    HTTP1ServerResponse & response;

    std::unique_ptr<WriteBufferFromPocoSocket> socket_out;

    bool is_http_method_head;

    bool headers_started_sending = false;
    bool headers_finished_sending = false;    /// If true, you could not add any headers.

    Progress accumulated_progress;
    Stopwatch progress_watch;

    int exception_code = 0;

    std::string exception_tag;

    std::mutex mutex;    /// progress callback could be called from different threads.
};

class HTTP1ServerResponse : public HTTPServerResponseBase
{
public:
    explicit HTTP1ServerResponse(
        Poco::Net::HTTPServerSession & session,
        const ProfileEvents::Event & write_event = ProfileEvents::end(),
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void setResponseDefaultHeaders() override;
    void drainRequestIfNeeded() override;

    void send100Continue() override;

    std::unique_ptr<WriteBufferFromHTTPServerResponseBase> makeUniqueStream() override;

    Poco::Net::HTTPServerSession & getSession() const noexcept { return session; }

    const HTTPServerRequest * getRequest() noexcept { return request; }

private:
    Poco::Net::HTTPServerSession & session;
    ProfileEvents::Event write_event;
    size_t buf_size;
};

}
