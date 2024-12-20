#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <memory>
#include <sstream>
#include <string>

namespace DB
{


void WriteBufferFromHTTPServerResponse::startSendHeaders()
{
    if (headers_started_sending)
        return;

    headers_started_sending = true;

    if (!response.getChunkedTransferEncoding() && response.getContentLength() == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH)
    {
        /// In case there is no Content-Length we cannot use keep-alive,
        /// since there is no way to know when the server send all the
        /// data, so "Connection: close" should be sent.
        response.setKeepAlive(false);
    }

    if (add_cors_header)
        response.set("Access-Control-Allow-Origin", "*");

    setResponseDefaultHeaders(response);

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response.beginWrite(header);
    auto header_str = header.str();
    socketSendBytes(header_str.data(), header_str.size());
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgressImpl(const char * header_name)
{
    if (is_http_method_head || headers_finished_sending || !headers_started_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;

    accumulated_progress.writeJSON(progress_string_writer);

    socketSendBytes(header_name, strlen(header_name));
    socketSendBytes(progress_string_writer.str().data(), progress_string_writer.str().size());
    socketSendBytes("\r\n", 2);
}

void WriteBufferFromHTTPServerResponse::writeHeaderSummary()
{
    accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
    writeHeaderProgressImpl("X-ClickHouse-Summary: ");
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgress()
{
    writeHeaderProgressImpl("X-ClickHouse-Progress: ");
}

void WriteBufferFromHTTPServerResponse::writeExceptionCode()
{
    if (headers_finished_sending || !exception_code)
        return;
    if (headers_started_sending)
    {
        socketSendBytes("X-ClickHouse-Exception-Code: ", sizeof("X-ClickHouse-Exception-Code: ") - 1);
        auto str_code = std::to_string(exception_code);
        socketSendBytes(str_code.data(), str_code.size());
        socketSendBytes("\r\n", 2);
    }
}

void WriteBufferFromHTTPServerResponse::finishSendHeaders()
{
    if (headers_finished_sending)
        return;

    if (!headers_started_sending)
    {
        if (compression_method != CompressionMethod::None)
            response.set("Content-Encoding", toContentEncodingName(compression_method));
        startSendHeaders();
    }

    writeHeaderSummary();
    writeExceptionCode();

    headers_finished_sending = true;

    /// Send end of headers delimiter.
    socketSendBytes("\r\n", 2);
}


void WriteBufferFromHTTPServerResponse::nextImpl()
{
    if (!initialized)
    {
        std::lock_guard lock(mutex);
        /// Initialize as early as possible since if the code throws,
        /// next() should not be called anymore.
        initialized = true;

        if (compression_method != CompressionMethod::None)
        {
            /// If we've already sent headers, just send the `Content-Encoding` down the socket directly
            if (headers_started_sending)
                socketSendStr("Content-Encoding: " + toContentEncodingName(compression_method) + "\r\n");
            else
                response.set("Content-Encoding", toContentEncodingName(compression_method));
        }

        startSendHeaders();
        finishSendHeaders();
    }

    if (!is_http_method_head)
        HTTPWriteBuffer::nextImpl();
}


WriteBufferFromHTTPServerResponse::WriteBufferFromHTTPServerResponse(
    HTTPServerResponse & response_,
    bool is_http_method_head_,
    const ProfileEvents::Event & write_event_)
    : HTTPWriteBuffer(response_.getSocket(), write_event_)
    , response(response_)
    , is_http_method_head(is_http_method_head_)
{
    if (response.getChunkedTransferEncoding())
        setChunked();
}


void WriteBufferFromHTTPServerResponse::onProgress(const Progress & progress)
{
    std::lock_guard lock(mutex);

    /// Cannot add new headers if body was started to send.
    if (headers_finished_sending)
        return;

    accumulated_progress.incrementPiecewiseAtomically(progress);
    if (send_progress && progress_watch.elapsed() >= send_progress_interval_ms * 1000000)
    {
        accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
        progress_watch.restart();

        /// Send all common headers before our special progress headers.
        startSendHeaders();
        writeHeaderProgress();
    }
}

void WriteBufferFromHTTPServerResponse::setExceptionCode(int exception_code_)
{
    std::lock_guard lock(mutex);
    if (headers_started_sending)
        exception_code = exception_code_;
    else
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code_));
}

WriteBufferFromHTTPServerResponse::~WriteBufferFromHTTPServerResponse()
{
    try
    {
        if (!canceled)
            finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromHTTPServerResponse::finalizeImpl()
{
    if (!headers_finished_sending)
    {
        std::lock_guard lock(mutex);
        /// If no body data just send header
        startSendHeaders();

        /// `finalizeImpl` must be idempotent, so set `initialized` here to not send stuff twice
        if (!initialized && offset() && compression_method != CompressionMethod::None)
        {
            initialized = true;
            socketSendStr("Content-Encoding: " + toContentEncodingName(compression_method) + "\r\n");
        }

        finishSendHeaders();
    }

    if (!is_http_method_head)
        HTTPWriteBuffer::finalizeImpl();
}


}
