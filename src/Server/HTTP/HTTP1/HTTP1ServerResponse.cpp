#include <Server/HTTP/HTTP1/HTTP1ServerResponse.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <IO/WriteBufferDecorator.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/NumberFormatter.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <fmt/core.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int REQUIRED_PASSWORD;
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

void WriteBufferFromHTTP1ServerResponse::sendBufferAndFinalize(const char * ptr, size_t size)
{
    {
        std::lock_guard lock(mutex);
        chassert(offset() == 0);
        chassert(!headers_started_sending);
        if (headers_started_sending)
            return;  /// FIXME: should throw an error here?
        response.setContentLength(size);
        response.setChunkedTransferEncoding(false);
        if (compression_method != CompressionMethod::None)
            response.set("Content-Encoding", toContentEncodingName(compression_method));
        startSendHeaders();
        finishSendHeaders(false);
    }

    if (!is_http_method_head)
        socket_out->socketSendBytes(ptr, size);
    finalize();
}

void WriteBufferFromHTTP1ServerResponse::startSendHeaders()
{
    if (headers_started_sending)
        return;

    headers_started_sending = true;
    response.markSendStarted();

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    header << response.getVersion() << " " << static_cast<int>(response.getStatus()) << " " << response.getReason() << "\r\n";
    header.flush();
    auto header_str = header.str();
    socket_out->socketSendBytes(header_str.data(), header_str.size());
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderProgressImpl(const char * header_name, Progress::DisplayMode mode)
{
    if (is_http_method_head || headers_finished_sending || !headers_started_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer, mode);
    progress_string_writer.finalize();

    socket_out->socketSendBytes(header_name, strlen(header_name));
    socket_out->socketSendBytes(progress_string_writer.str().data(), progress_string_writer.str().size());
    socket_out->socketSendBytes("\r\n", 2);
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderSummary()
{
    accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
    /// Write the verbose summary with all the zero values included, if any.
    /// This is needed for compatibility with an old version of the third-party ClickHouse driver for Elixir.
    writeHeaderProgressImpl("X-ClickHouse-Summary: ", Progress::DisplayMode::Verbose);
}

void WriteBufferFromHTTP1ServerResponse::writeHeaderProgress()
{
    writeHeaderProgressImpl("X-ClickHouse-Progress: ", Progress::DisplayMode::Minimal);
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

void WriteBufferFromHTTP1ServerResponse::finishSendHeaders(bool send_exception_tag)
{
    if (headers_finished_sending)
        return;

    if (!headers_started_sending)
    {
        startSendHeaders();
    }

    if (count() && compression_method != CompressionMethod::None)
        response.set("Content-Encoding", toContentEncodingName(compression_method));

    if (add_cors_header)
        response.set("Access-Control-Allow-Origin", "*");

    if (send_exception_tag)
        response.set("X-ClickHouse-Exception-Tag", exception_tag);

    writeHeaderSummary();
    writeExceptionCode();

    /// FIXME: next() works only for chunked transfer and HTTP/1.0 "end on connection close" transfer
    /// Should check response headers for explicitly set Content-Length and do smth with it
    if (!response.getChunkedTransferEncoding() && response.getContentLength() == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH)
    {
        /// In case there is no Content-Length we cannot use keep-alive,
        /// since there is no way to know when the server send all the
        /// data, so "Connection: close" should be sent.
        response.setKeepAlive(false);
    }

    /// FIXME: keep-alive handling might require a redesign
    /// Connection can only be reused if we've fully read the previous request and all its POST data.
    /// Otherwise we'd misinterpret the leftover data as part of the next request's header.
    /// HTTPServerRequest::canKeepAlive() checks that request stream is bounded and is fully read.
    /// FIXME: the HTTP_CONTINUE check is broken because status is never HTTP_CONTINUE
    if (
        !response.getRequest()
        || (response.getRequest()->getExpectContinue() && response.getStatus() != Poco::Net::HTTPResponse::HTTP_CONTINUE)
        || !response.getRequest()->canKeepAlive()
    )
        response.setKeepAlive(false);

    response.setResponseDefaultHeaders();

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response.Poco::Net::MessageHeader::write(header);
    header << "\r\n";
    header.flush();
    auto header_str = header.str();
    socket_out->socketSendBytes(header_str.data(), header_str.size());

    headers_finished_sending = true;
}


void WriteBufferFromHTTP1ServerResponse::nextImpl()
{
    {
        std::lock_guard lock(mutex);
        startSendHeaders();
        finishSendHeaders();
    }

    if (offset() == 0)
        return;

    if (response.getStatus() == HTTPResponse::HTTP_NOT_MODIFIED || response.getStatus() == HTTPResponse::HTTP_NO_CONTENT)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "304 and 204 responses must not contain a body");

    if (is_http_method_head)
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
    /// FIXME: better handle the presence of Content-Length
}


WriteBufferFromHTTP1ServerResponse::WriteBufferFromHTTP1ServerResponse(
    HTTP1ServerResponse & response_,
    bool is_http_method_head_,
    const ProfileEvents::Event & write_event_,
    size_t buf_size_)
    : WriteBufferFromHTTPServerResponseBase(buf_size_)
    , response(response_)
    , socket_out(std::make_unique<WriteBufferFromPocoSocket>(response.getSession().socket(), write_event_))
    , is_http_method_head(is_http_method_head_)
    , exception_tag(getRandomASCIIString(EXCEPTION_TAG_LENGTH))
{
}


void WriteBufferFromHTTP1ServerResponse::onProgress(const Progress & progress, ContextPtr context)
{
    std::lock_guard lock(mutex);

    /// Cannot add new headers if body was started to send.
    if (headers_finished_sending)
        return;

    if (progress.memory_usage)
    {
        /// When the query is finished the memory_usage will be in progress and it will match the query_log
        accumulated_progress.memory_usage = 0;
    }
    else
    {
        QueryStatusPtr process_list_elem = context->getProcessListElement();
        if (process_list_elem)
            accumulated_progress.memory_usage = process_list_elem->getInfo().memory_usage;
    }

    accumulated_progress.incrementPiecewiseAtomically(progress);
    if (send_progress && (progress_watch.elapsed() >= send_progress_interval_ms * 1000000))
    {
        accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
        progress_watch.restart();

        try {
            /// Do not send headers before our special progress headers
            /// For example, header "Connection: close|keep-alive" is defined only right before sending response
            startSendHeaders();
            writeHeaderProgress();
        }
        catch (...)
        {
            cancel();
            throw;
        }
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

void WriteBufferFromHTTP1ServerResponse::finalizeImpl()
{
    {
        std::lock_guard lock(mutex);
        startSendHeaders();
        finishSendHeaders();
    }

    chassert(socket_out->offset() == 0);   /// Because we do not actually use socket_out's buffer
    socket_out->finalize();   /// So it should be a no-op

    if (is_http_method_head)
        return;

    WriteBufferFromHTTPServerResponseBase::finalizeImpl();
    if (response.getChunkedTransferEncoding())
        socket_out->socketSendBytes("0\r\n\r\n", 5);
}

void WriteBufferFromHTTP1ServerResponse::cancelImpl() noexcept
{
    // we have to close connection when it has been canceled
    // client is waiting final empty chunk in the chunk encoding, chient has to receive EOF instead it ASAP
    response.setKeepAlive(false);
    chassert(socket_out->offset() == 0);   /// Because we do not actually use socket_out's buffer
    socket_out->cancel();
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
        // proper sending bad http code
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
            // but make sense to try to send proper `connection: close` header if headers are not finished yet
            response.setKeepAlive(false);
            response.getSession().setKeepAlive(false);

            // try to send proper header in case headers are not finished yet
            setExceptionCode(exception_code_);

            // this starts new chunk in case of Transfer-Encoding: chunked
            if (use_compression_buffer)
                compression_buffer->next();
            next();

            auto & out = use_compression_buffer ? *compression_buffer : *this;

            // Write the exception block in response in new format as follows.
            // The whole exception block should not exceed MAX_EXCEPTION_SIZE
            // __exception__
            // <TAG>
            // <error message of max 16K bytes>
            // <message_length> <TAG>
            // __exception__

            // 2 bytes represents - \r\n
            // 1 byte represents - ' ' (space between <message_length> <TAG>) in the above exception block format
            // 8 byte represents - <message_length>
            size_t size_message_excluded = 2 + EXCEPTION_MARKER.size() + 2 + EXCEPTION_TAG_LENGTH + 2 + 8 + 1 + EXCEPTION_TAG_LENGTH + 2 + EXCEPTION_MARKER.size() + 2;

            size_t max_exception_message_size = MAX_EXCEPTION_SIZE - size_message_excluded;

            writeCString("\r\n", out);
            writeString(EXCEPTION_MARKER, out);
            writeCString("\r\n", out);
            writeString(exception_tag, out);
            writeCString("\r\n", out);

            std::string limited_message = message;
            if (limited_message.size() > max_exception_message_size)
                limited_message = limited_message.substr(0, max_exception_message_size);

            writeString(limited_message, out);
            if (!limited_message.ends_with('\n'))
                writeChar('\n', out);

            writeIntText(limited_message.size() + (limited_message.ends_with('\n') ? 0 : 1), out);
            writeChar(' ', out);
            writeString(exception_tag, out);
            writeCString("\r\n", out);
            writeString(EXCEPTION_MARKER, out);
            writeCString("\r\n", out);

            if (use_compression_buffer)
                compression_buffer->next();
            next();

            LOG_DEBUG(
                getLogger("WriteBufferFromHTTP1ServerResponse"),
                "Write buffer has been canceled with an error."
                "Error has been sent at the end of the response. The server tried to break the HTTP protocol."
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
    if (!request)
        return;

    auto input_stream = request->getStream();
    if (input_stream->isCanceled())
    {
        setKeepAlive(false);
        LOG_WARNING(getLogger("drainRequestIfNeeded"), "Cannot read remaining request body during exception handling. The request read buffer is canceled. Set keep alive to false on the response.");
        return;
    }

    LOG_DEBUG(getLogger("drainRequestIfNeeded"), "Draining connection ({}, Transfer-Encoding: {}, Content-Length: {}, Keep-Alive: {})",
              request->getVersion(), request->getTransferEncoding(), request->getContentLength(), request->getKeepAlive());

    /// If HTTP method is POST and Keep-Alive is turned on, we should try to read the whole request body
    /// to avoid reading part of the current request body in the next request.
    /// Or we have to close connection after this request.
    if (request->getMethod() == Poco::Net::HTTPRequest::HTTP_POST
        && (request->getChunkedTransferEncoding() || request->hasContentLength())
        && getKeepAlive())
    {
        /// If the client expects 100 Continue, but we never sent it, don't attempt to read the body and
        /// don't reuse the connection.
        if (request->getExpectContinue() && getStatus() != Poco::Net::HTTPResponse::HTTP_CONTINUE)
        {
            setKeepAlive(false);
        }
        else
        {
            try
            {
                if (!input_stream->eof())
                {
                    size_t ignored_bytes = input_stream->ignoreAll();
                    LOG_DEBUG(getLogger("sendExceptionToHTTPClient"), "Drained {} bytes", ignored_bytes);
                }
            }
            catch (...)
            {
                tryLogCurrentException("sendExceptionToHTTPClient", "Cannot read remaining request body during exception handling. Set keep alive to false on the response.");
                setKeepAlive(false);
            }
        }
    }
}

std::unique_ptr<WriteBufferFromHTTPServerResponseBase> HTTP1ServerResponse::makeUniqueStream()
{
    return std::make_unique<WriteBufferFromHTTP1ServerResponse>(
        *this, request && request->getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, write_event, buf_size);
}

}
