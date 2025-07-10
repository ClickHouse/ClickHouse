#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>
#include <DataTypes/IDataType.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Poco/Net/HTTPResponse.h>

#include <fmt/core.h>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int CANNOT_WRITE_TO_SOCKET;
    extern const int REQUIRED_PASSWORD;
    extern const int ABORTED;
}


void WriteBufferFromHTTPServerResponse::startSendHeaders()
{
    if (headers_started_sending)
        return;

    headers_started_sending = true;

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response.writeStatus(header);
    auto header_str = header.str();
    socketSendBytes(header_str.data(), header_str.size());
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgressImpl(const char * header_name, Progress::DisplayMode mode)
{
    if (is_http_method_head || headers_finished_sending || !headers_started_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer, mode);
    progress_string_writer.finalize();

    socketSendBytes(header_name, strlen(header_name));
    socketSendBytes(progress_string_writer.str().data(), progress_string_writer.str().size());
    socketSendBytes("\r\n", 2);
}

void WriteBufferFromHTTPServerResponse::writeHeaderSummary()
{
    accumulated_progress.incrementElapsedNs(progress_watch.elapsed());
    /// Write the verbose summary with all the zero values included, if any.
    /// This is needed for compatibility with an old version of the third-party ClickHouse driver for Elixir.
    writeHeaderProgressImpl("X-ClickHouse-Summary: ", Progress::DisplayMode::Verbose);
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgress()
{
    writeHeaderProgressImpl("X-ClickHouse-Progress: ", Progress::DisplayMode::Minimal);
}

void WriteBufferFromHTTPServerResponse::writeExceptionCode()
{
    if (headers_finished_sending || !exception_code)
        return;

    if (headers_started_sending)
    {
        static std::string_view header_key = "X-ClickHouse-Exception-Code: ";
        socketSendBytes(header_key.data(), header_key.size());
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
        startSendHeaders();
    }

    setResponseDefaultHeaders(response);

    if (count() && compression_method != CompressionMethod::None)
        response.set("Content-Encoding", toContentEncodingName(compression_method));

    if (add_cors_header)
        response.set("Access-Control-Allow-Origin", "*");

    writeHeaderSummary();
    writeExceptionCode();

    std::stringstream header; //STYLE_CHECK_ALLOW_STD_STRING_STREAM
    response.writeHeaders(header);
    auto header_str = header.str();
    socketSendBytes(header_str.data(), header_str.size());

    headers_finished_sending = true;
}


void WriteBufferFromHTTPServerResponse::nextImpl()
{
    {
        std::lock_guard lock(mutex);
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

void WriteBufferFromHTTPServerResponse::setExceptionCode(int code)
{
    std::lock_guard lock(mutex);

    if (headers_started_sending)
        exception_code = code;
    else
        response.set("X-ClickHouse-Exception-Code", toString<int>(code));

    if (code == ErrorCodes::REQUIRED_PASSWORD)
        response.requireAuthentication("ClickHouse server HTTP API");
    else
        response.setStatusAndReason(exceptionCodeToHTTPStatus(code));
}

void WriteBufferFromHTTPServerResponse::finalizeImpl()
{
    {
        std::lock_guard lock(mutex);
        startSendHeaders();
        finishSendHeaders();
    }

    if (!is_http_method_head)
    {
        HTTPWriteBuffer::finalizeImpl();
    }
}

void WriteBufferFromHTTPServerResponse::cancelImpl() noexcept
{
    // we have to close connection when it has been canceled
    // client is waiting final empty chunk in the chunk encoding, chient has to receive EOF instead it ASAP
    response.setKeepAlive(false);
    HTTPWriteBuffer::cancelImpl();
}

bool WriteBufferFromHTTPServerResponse::isChunked() const
{
    chassert(response.sent());
    return HTTPWriteBuffer::isChunked();
}

bool WriteBufferFromHTTPServerResponse::isFixedLength() const
{
    chassert(response.sent());
    return HTTPWriteBuffer::isFixedLength();
}

bool WriteBufferFromHTTPServerResponse::cancelWithException(HTTPServerRequest & request, int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept
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

        bool is_response_sent = response.sent();
        // proper senging bad http code
        if (!is_response_sent)
        {
            drainRequestIfNeeded(request, response);
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
                getLogger("WriteBufferFromHTTPServerResponse"),
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
            // In case of fixed lengs we could send error but less that fixedLengthLeft bytes.
            // In case of plain stream all ways are questionable, but lets send the error any way.

            // no point to drain request, transmission has been already started hence the request has been read
            // but make sense to try to send proper `connnection: close` header if headers are not finished yet
            response.setKeepAlive(false);

            // try to send proper header in case headers are not finished yet
            setExceptionCode(exception_code_);

            // this starts new chunk in case of Transfer-Encoding: chunked
            if (use_compression_buffer)
                compression_buffer->next();
            next();

            if (isFixedLength())
            {
                if (fixedLengthLeft() > EXCEPTION_MARKER.size())
                {
                    // fixed length buffer drops all excess data
                    // make sure that we send less than content-lenght bytes at the end
                    // the aim is to break HTTP
                    breakFixedLength();
                }
                else if (fixedLengthLeft() > 0)
                {
                    breakFixedLength();
                    throw Exception(ErrorCodes::CANNOT_WRITE_TO_SOCKET,
                        "There is no space left in the fixed length HTTP-write buffer to write the exception header."
                        "But the client should notice the broken HTTP protocol.");
                }
                else
                {
                    throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER,
                        "The response has been fully sent to the client already. That error not have affected the response.");
                }
            }
            else
            {
                // Otherwise server is unable to broke HTTP protocol
                chassert(isChunked());
            }

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
                getLogger("WriteBufferFromHTTPServerResponse"),
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

}
