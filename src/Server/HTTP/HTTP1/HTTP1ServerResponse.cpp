#include <Common/logger_useful.h>

#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/HTTP1/HTTP1ServerResponse.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/NumberFormatter.h>

#include <memory>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int REQUIRED_PASSWORD;
    extern const int ABORTED;
}

static constexpr std::string_view EXCEPTION_MARKER = "__exception__";

WriteBufferFromHTTP1ServerResponse::WriteBufferFromHTTP1ServerResponse(
    HTTP1ServerResponse & response_,
    bool is_http_method_head_,
    const ProfileEvents::Event & write_event_,
    size_t buf_size_)
    : WriteBufferFromHTTPServerResponseBase(buf_size_)
    , response(response_)
    , socket_out(std::make_unique<WriteBufferFromPocoSocket>(response.getSession().socket(), write_event_))
    , is_http_method_head(is_http_method_head_)
{
}

void WriteBufferFromHTTP1ServerResponse::sendBufferAndFinalize(const char * ptr, size_t size)
{
    chassert(offset() == 0);
    chassert(!headers_started_sending);
    if (headers_started_sending)
    {
        /// FIXME: should throw an error here?
        return;
    }
    response.setContentLength(size);
    response.setChunkedTransferEncoding(false);
    if (compression_method != CompressionMethod::None)
        response.set("Content-Encoding", toContentEncodingName(compression_method));
    startSendHeaders();
    finishSendHeaders();
    if (!is_http_method_head)
        socket_out->socketSendBytes(ptr, size);
    finalize();
}

void WriteBufferFromHTTP1ServerResponse::onProgress(const Progress & progress)
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

void WriteBufferFromHTTP1ServerResponse::setExceptionCode(int code)
{
    std::lock_guard lock(mutex);

    if (headers_started_sending)
        exception_code = code;
    else
        response.set("X-ClickHouse-Exception-Code", toString<int>(code));

    if (code == ErrorCodes::REQUIRED_PASSWORD)
    {
        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.set("WWW-Authenticate", "Basic realm=\"ClickHouse server HTTP API\"");
    }
    else
        response.setStatusAndReason(exceptionCodeToHTTPStatus(code));
}

bool WriteBufferFromHTTP1ServerResponse::cancelWithException(int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept
{
    bool use_compression_buffer = compression_buffer && !compression_buffer->isCanceled() && !compression_buffer->isFinalized();

    try
    {
        if (isCanceled())
            throw Exception(ErrorCodes::ABORTED, "Write buffer has been canceled.");

        if (isFinalized())
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "Write buffer has been finalized.");

        bool data_sent = false;
        size_t compression_discarded_data = 0;
        size_t discarded_data = 0;
        if  (use_compression_buffer)
        {
            data_sent |= (compression_buffer->count() != compression_buffer->offset());
            if (!data_sent)
                compression_discarded_data = compression_buffer->rejectBufferedDataSave();
        }
        data_sent |= (count() != offset());
        if (!data_sent)
            discarded_data = rejectBufferedDataSave();

        bool is_response_sent = response.sendStarted();
        // proper senging bad http code
        if (!is_response_sent)
        {
            response.drainRequestIfNeeded();
            // We try to send the exception message when the transmission has not been started yet
            // Set HTTP code and HTTP message. Add "X-ClickHouse-Exception-Code" header.
            // If it is not HEAD request send the message in the body.
            setExceptionCode(exception_code_);

            auto & out = use_compression_buffer ? *compression_buffer : *this;
            writeString(message, out);
            if (!message.ends_with('\n'))
                writeChar('\n', out);

            if (use_compression_buffer)
                compression_buffer->finalize();
            finalize();

            LOG_DEBUG(
                getLogger("WriteBufferFromHTTP1ServerResponse"),
                "Write buffer has been canceled with an error."
                " Proper HTTP error code and headers have been send to the client."
                " HTTP code: {}, message: <{}>, error code: {}, message: <{}>,"
                " use compression: {}, data has been send through buffers: {}, compression discarded data: {}, discarded data: {}",
            response.getStatus(), response.getReason(), exception_code_, message,
            use_compression_buffer,
            data_sent, compression_discarded_data, discarded_data);
        }
        else
        {
            // We try to send the exception message even when the transmission has been started already
            // In case the chunk encoding: send new chunk which started with CHUNK_ENCODING_ERROR_HEADER and contains the error description
            // it is important to avoid sending the last empty chunk in order to break the http protocol here.
            // In case of plain stream all ways are questionable, but lets send the error any way.

            // no point to drain request, transmission has been already started hence the request has been read
            // but make sense to try to send proper `connnection: close` header if headers are not finished yet
            response.setKeepAlive(false);
            response.getSession().setKeepAlive(false);

            // try to send proper header in case headers are not finished yet
            setExceptionCode(exception_code_);

            // this starts new chunk in case of Transfer-Encoding: chunked
            if (use_compression_buffer)
                compression_buffer->next();
            next();

            auto & out = use_compression_buffer ? *compression_buffer : *this;
            writeString(EXCEPTION_MARKER, out);
            writeCString("\r\n", out);
            writeString(message, out);
            if (!message.ends_with('\n'))
                writeChar('\n', out);

            // this finish chunk with the error message in case of Transfer-Encoding: chunked
            if (use_compression_buffer)
                compression_buffer->next();
            next();

            LOG_DEBUG(
                getLogger("WriteBufferFromHTTP1ServerResponse"),
                "Write buffer has been canceled with an error."
                "Error has been sent at the end of the response. HTTP protocol has been broken by server."
                " HTTP code: {}, message: <{}>, error code: {}, message: <{}>."
                " use compression: {}, data has been send through buffers: {}, compression discarded data: {}, discarded data: {}",
            response.getStatus(), response.getReason(), exception_code_, message,
            use_compression_buffer,
            data_sent, compression_discarded_data, discarded_data);

            // this prevent sending final empty chunk in case of Transfer-Encoding: chunked
            // the aim is to break HTTP
            if (use_compression_buffer)
                compression_buffer->cancel();
            cancel();
        }

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to send exception to the response write buffer.");

        if (use_compression_buffer)
            compression_buffer->cancel();

        cancel();

        return false;
    }
}

void WriteBufferFromHTTP1ServerResponse::startSendHeaders()
{
    if (headers_started_sending)
        return;

    headers_started_sending = true;

    /// FIXME: next() works only for chunked transfer and HTTP/1.0 "end on connection close" transfer
    /// Should check response headers for explicitly set Content-Length and do smth with it

    if (!response.getChunkedTransferEncoding() && response.getContentLength() == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH)
    {
        /// In case there is no Content-Length we cannot use keep-alive,
        /// since there is no way to know when the server send all the
        /// data, so "Connection: close" should be sent.
        response.setKeepAlive(false);
    }

    if (add_cors_header)
        response.set("Access-Control-Allow-Origin", "*");

    response.setResponseDefaultHeaders();

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response.beginWrite(header);
    response.markSendStarted();
    auto header_str = header.str();
    socket_out->socketSendBytes(header_str.data(), header_str.size());
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderProgressImpl(const char * header_name)
{
    if (is_http_method_head || headers_finished_sending || !headers_started_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer);
    progress_string_writer.finalize();

    socket_out->socketSendBytes(header_name, strlen(header_name));
    socket_out->socketSendBytes(progress_string_writer.str().data(), progress_string_writer.str().size());
    socket_out->socketSendBytes("\r\n", 2);
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderSummary()
{
    accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
    writeHeaderProgressImpl("X-ClickHouse-Summary: ");
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderProgress()
{
    writeHeaderProgressImpl("X-ClickHouse-Progress: ");
}

void WriteBufferFromHTTP1ServerResponse::writeExceptionCode()
{
    if (headers_finished_sending || !exception_code)
        return;

    if (headers_started_sending)
    {
        static std::string_view header_key = "X-ClickHouse-Exception-Code: ";
        socket_out->socketSendBytes(header_key.data(), header_key.size());
        auto str_code = std::to_string(exception_code);
        socket_out->socketSendBytes(str_code.data(), str_code.size());
        socket_out->socketSendBytes("\r\n", 2);
    }
}

void WriteBufferFromHTTP1ServerResponse::finishSendHeaders()
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
    socket_out->socketSendBytes("\r\n", 2);
}

void WriteBufferFromHTTP1ServerResponse::nextImpl()
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
                socket_out->socketSendStr("Content-Encoding: " + toContentEncodingName(compression_method) + "\r\n");
            else
                response.set("Content-Encoding", toContentEncodingName(compression_method));
        }

        startSendHeaders();
        finishSendHeaders();
    }

    if (is_http_method_head || offset() == 0)
        return;

    if (response.getChunkedTransferEncoding())
    {
        std::string chunk_header;
        Poco::NumberFormatter::appendHex(chunk_header, offset());
        chunk_header.append("\r\n", 2);
        socket_out->socketSendBytes(chunk_header.data(), static_cast<int>(chunk_header.size()));
        socket_out->socketSendBytes(working_buffer.begin(), offset());
        socket_out->socketSendBytes("\r\n", 2);
    }
    else
        socket_out->socketSendBytes(working_buffer.begin(), offset());
}

void WriteBufferFromHTTP1ServerResponse::finalizeImpl()
{
    chassert(socket_out->offset() == 0);
    socket_out->finalize();

    if (!headers_finished_sending)
    {
        std::lock_guard lock(mutex);
        /// If no body data just send header
        startSendHeaders();

        /// `finalizeImpl` must be idempotent, so set `initialized` here to not send stuff twice
        if (!initialized && offset() && compression_method != CompressionMethod::None)
        {
            initialized = true;
            socket_out->socketSendStr("Content-Encoding: " + toContentEncodingName(compression_method) + "\r\n");
        }

        finishSendHeaders();
    }

    if (is_http_method_head)
        return;

    nextImpl();
    if (response.getChunkedTransferEncoding())
        socket_out->socketSendBytes("0\r\n\r\n", 5);
}

void WriteBufferFromHTTP1ServerResponse::cancelImpl() noexcept
{
    /// FIXME: Is it correct? When do we use it?

    // we have to close connection when it has been canceled
    // client is waiting final empty chunk in the chunk encoding, chient has to receive EOF instead it ASAP
    response.setKeepAlive(false);
    chassert(socket_out->offset() == 0);
    socket_out->cancel();
}

HTTP1ServerResponse::HTTP1ServerResponse(
    Poco::Net::HTTPServerSession & session_,
    const ProfileEvents::Event & write_event_,
    size_t buf_size_)
    : session(session_), write_event(write_event_), buf_size(buf_size_)
{
}

void HTTP1ServerResponse::send100Continue()
{
    Poco::Net::HTTPHeaderOutputStream hs(session);
    hs << getVersion() << " 100 Continue\r\n\r\n";
}

void HTTP1ServerResponse::setResponseDefaultHeaders()
{
    if (!getKeepAlive())
        return;

    const size_t keep_alive_timeout = session.getKeepAliveTimeout();
    const size_t keep_alive_max_requests = session.getMaxKeepAliveRequests();
    if (keep_alive_timeout)
    {
        if (keep_alive_max_requests)
            set("Keep-Alive", fmt::format("timeout={}, max={}", keep_alive_timeout, keep_alive_max_requests));
        else
            set("Keep-Alive", fmt::format("timeout={}", keep_alive_timeout));
    }
}

void HTTP1ServerResponse::drainRequestIfNeeded()
{
    /// If HTTP method is POST and Keep-Alive is turned on, we should try to read the whole request body
    /// to avoid reading part of the current request body in the next request.
    /// Or we have to close connection after this request.
    if (request && request->getMethod() == Poco::Net::HTTPRequest::HTTP_POST
        && (request->getChunkedTransferEncoding() || request->hasContentLength())
        && getKeepAlive())
    {
        try
        {
            if (!request->getStream().eof())
                request->getStream().ignoreAll();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Cannot read remaining request body of HTTP/1 request during exception handling. Set keep alive to false on the response.");
            setKeepAlive(false);
        }
    }
}

WriteBufferFromHTTPServerResponseBase * HTTP1ServerResponse::makeNewStream() noexcept
{
    return new WriteBufferFromHTTP1ServerResponse(  // NOLINT
        *this, request && request->getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, write_event, buf_size);
}

}
