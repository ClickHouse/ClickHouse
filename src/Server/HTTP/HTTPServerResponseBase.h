#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/Progress.h>

#include <Server/HTTP/HTTPResponse.h>

namespace DB
{

class HTTPServerRequest;

class WriteBufferFromHTTPServerResponseBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromHTTPServerResponseBase(size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<WriteBuffer>(buf_size)
    {
    }

    /// NOTE: these methods do not send a (redundant) X-ClickHouse-Exception-Tag header
    virtual void sendBufferAndFinalize(const char * ptr, size_t size) = 0;
    void sendBufferAndFinalize(std::string_view sv) { sendBufferAndFinalize(sv.data(), sv.size()); }

    /// FIXME: Works only for HTTP/1.x. It actually violates the RFC 9112 section 5.1. a bit
    /// Writes progress in repeating HTTP headers.
    virtual void onProgress(const Progress & /*progress*/, ContextPtr /*context*/) {}

    /// FIXME: why don't just set an appropriate header instead of calling this method?
    /// Turn CORS on or off.
    /// The setting has any effect only if HTTP headers haven't been sent yet.
    void addHeaderCORS(bool enable_cors)
    {
        add_cors_header = enable_cors;
    }

    void setSendProgress(bool send_progress_) { send_progress = send_progress_; }

    /// Don't send HTTP headers with progress more frequently.
    void setSendProgressInterval(size_t send_progress_interval_ms_)
    {
        send_progress_interval_ms = send_progress_interval_ms_;
    }

    void setCompressionMethodHeader(const CompressionMethod & compression_method_)
    {
        compression_method = compression_method_;
    }

    virtual void setExceptionCode(int code) = 0;

    virtual bool cancelWithException(int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept = 0;

protected:
    bool add_cors_header = false;
    bool send_progress = false;
    size_t send_progress_interval_ms = 100;
    CompressionMethod compression_method = CompressionMethod::None;
};

class HTTPServerResponseBase : public HTTPResponse
{
public:
    void attachRequest(const HTTPServerRequest * request_) { request = request_; }

    /// FIXME: wrapping this logic into a virtual method looks ugly. It's used only for HTTP/1.x
    virtual void setResponseDefaultHeaders() {}
    virtual void drainRequestIfNeeded() {}

    /// FIXME: better interim (1xx) responses support
    virtual void send100Continue() = 0;

    bool sendStarted() const noexcept { return send_started; }
    void markSendStarted() noexcept { send_started = true; }

    /// FIXME: ugly interface. You should never want to call these methods more then once
    virtual std::unique_ptr<WriteBufferFromHTTPServerResponseBase> makeUniqueStream() = 0;
    std::shared_ptr<WriteBufferFromHTTPServerResponseBase> makeStream() { return std::shared_ptr<WriteBufferFromHTTPServerResponseBase>(makeUniqueStream().release()); }

protected:
    const HTTPServerRequest * request = nullptr;

private:
    bool send_started = false;   /// Does not count interim (1xx) responses
};

}
