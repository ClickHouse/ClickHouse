#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <IO/WriteBuffer.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Poco/Net/StreamSocket.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>

#include <mutex>
#include <optional>


namespace DB
{

/// Postpone sending HTTP header until first data is flushed. This is needed in HTTP servers
///  to change some HTTP headers (e.g. response code) before any data is sent to the client.
///
/// Also this class write and flush special X-ClickHouse-Progress HTTP headers
///  if no data was sent at the time of progress notification.
/// This allows to implement progress bar in HTTP clients.
class WriteBufferFromHTTPServerResponse final : public HTTPWriteBuffer
{
public:
    WriteBufferFromHTTPServerResponse(
        HTTPServerResponse & response_,
        bool is_http_method_head_,
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    ~WriteBufferFromHTTPServerResponse() override;

    /// Writes progress in repeating HTTP headers.
    void onProgress(const Progress & progress);

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

    void setExceptionCode(int exception_code_);

private:
    /// Send at least HTTP headers if no data has been sent yet.
    /// Use after the data has possibly been sent and no error happened (and thus you do not plan
    /// to change response HTTP code.
    /// This method is idempotent.
    void finalizeImpl() override;

    /// Must be called under locked mutex.
    /// This method send headers, if this was not done already,
    ///  but not finish them with \r\n, allowing to send more headers subsequently.
    void startSendHeaders();

    /// Used to write the header X-ClickHouse-Progress / X-ClickHouse-Summary
    void writeHeaderProgressImpl(const char * header_name);
    /// Used to write the header X-ClickHouse-Progress
    void writeHeaderProgress();
    /// Used to write the header X-ClickHouse-Summary
    void writeHeaderSummary();
    /// Use to write the header X-ClickHouse-Exception-Code even when progress has been sent
    void writeExceptionCode();

    /// This method finish headers with \r\n, allowing to start to send body.
    void finishSendHeaders();

    void nextImpl() override;

    HTTPServerResponse & response;

    bool is_http_method_head;
    bool add_cors_header = false;

    bool initialized = false;

    bool headers_started_sending = false;
    bool headers_finished_sending = false;    /// If true, you could not add any headers.

    Progress accumulated_progress;
    bool send_progress = false;
    size_t send_progress_interval_ms = 100;
    Stopwatch progress_watch;

    CompressionMethod compression_method = CompressionMethod::None;

    int exception_code = 0;

    std::mutex mutex;    /// progress callback could be called from different threads.
};

}
