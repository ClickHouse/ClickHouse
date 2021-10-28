#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/MemoryTracker.h>

#include <Common/config.h>

#include <Poco/Version.h>


namespace DB
{

namespace ErrorCodes
{
}


void WriteBufferFromHTTPServerResponse::startSendHeaders()
{
    if (!headers_started_sending)
    {
        headers_started_sending = true;

        if (add_cors_header)
            response.set("Access-Control-Allow-Origin", "*");

        setResponseDefaultHeaders(response, keep_alive_timeout);

        if (!is_http_method_head)
            std::tie(response_header_ostr, response_body_ostr) = response.beginSend();
    }
}

void WriteBufferFromHTTPServerResponse::writeHeaderSummary()
{
    if (headers_finished_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer);

    if (response_header_ostr)
        *response_header_ostr << "X-ClickHouse-Summary: " << progress_string_writer.str() << "\r\n" << std::flush;
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgress()
{
    if (headers_finished_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer);

    if (response_header_ostr)
        *response_header_ostr << "X-ClickHouse-Progress: " << progress_string_writer.str() << "\r\n" << std::flush;
}

void WriteBufferFromHTTPServerResponse::finishSendHeaders()
{
    if (!headers_finished_sending)
    {
        writeHeaderSummary();
        headers_finished_sending = true;

        if (!is_http_method_head)
        {
            /// Send end of headers delimiter.
            if (response_header_ostr)
                *response_header_ostr << "\r\n" << std::flush;
        }
        else
        {
            if (!response_body_ostr)
                response_body_ostr = response.send();
        }
    }
}


void WriteBufferFromHTTPServerResponse::nextImpl()
{
    if (!initialized)
    {
        std::lock_guard lock(mutex);

        /// Initialize as early as possible since if the code throws,
        /// next() should not be called anymore.
        initialized = true;

        startSendHeaders();

        if (!out && !is_http_method_head)
        {
            if (compress)
            {
                auto content_encoding_name = toContentEncodingName(compression_method);

                *response_header_ostr << "Content-Encoding: " << content_encoding_name << "\r\n";
            }

            /// We reuse our buffer in "out" to avoid extra allocations and copies.

            if (compress)
                out = wrapWriteBufferWithCompressionMethod(
                    std::make_unique<WriteBufferFromOStream>(*response_body_ostr),
                    compress ? compression_method : CompressionMethod::None,
                    compression_level,
                    working_buffer.size(),
                    working_buffer.begin());
            else
                out = std::make_unique<WriteBufferFromOStream>(
                    *response_body_ostr,
                    working_buffer.size(),
                    working_buffer.begin());
        }

        finishSendHeaders();
    }

    if (out)
    {
        out->buffer() = buffer();
        out->position() = position();
        out->next();
    }
}


WriteBufferFromHTTPServerResponse::WriteBufferFromHTTPServerResponse(
    HTTPServerResponse & response_,
    bool is_http_method_head_,
    unsigned keep_alive_timeout_,
    bool compress_,
    CompressionMethod compression_method_)
    : BufferWithOwnMemory<WriteBuffer>(DBMS_DEFAULT_BUFFER_SIZE)
    , response(response_)
    , is_http_method_head(is_http_method_head_)
    , keep_alive_timeout(keep_alive_timeout_)
    , compress(compress_)
    , compression_method(compression_method_)
{
}


void WriteBufferFromHTTPServerResponse::onProgress(const Progress & progress)
{
    std::lock_guard lock(mutex);

    /// Cannot add new headers if body was started to send.
    if (headers_finished_sending)
        return;

    accumulated_progress.incrementPiecewiseAtomically(progress);

    if (progress_watch.elapsed() >= send_progress_interval_ms * 1000000)
    {
        progress_watch.restart();

        /// Send all common headers before our special progress headers.
        startSendHeaders();
        writeHeaderProgress();
    }
}


void WriteBufferFromHTTPServerResponse::finalize()
{
    try
    {
        next();
        if (out)
            out->finalize();
        out.reset();
        /// Catch write-after-finalize bugs.
        set(nullptr, 0);
    }
    catch (...)
    {
        /// Avoid calling WriteBufferFromOStream::next() from dtor
        /// (via WriteBufferFromHTTPServerResponse::next())
        out.reset();
        throw;
    }

    if (!offset())
    {
        /// If no remaining data, just send headers.
        std::lock_guard lock(mutex);
        startSendHeaders();
        finishSendHeaders();
    }
}


WriteBufferFromHTTPServerResponse::~WriteBufferFromHTTPServerResponse()
{
    /// FIXME move final flush into the caller
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    finalize();
}

}
