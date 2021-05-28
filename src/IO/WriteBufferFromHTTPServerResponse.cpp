#include <Poco/Version.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteBufferFromString.h>
#include <IO/HTTPCommon.h>
#include <IO/Progress.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif


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

#if defined(POCO_CLICKHOUSE_PATCH)
        if (request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
            std::tie(response_header_ostr, response_body_ostr) = response.beginSend();
#endif
    }
}

void WriteBufferFromHTTPServerResponse::writeHeaderSummary()
{
#if defined(POCO_CLICKHOUSE_PATCH)
    if (headers_finished_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer);

    if (response_header_ostr)
        *response_header_ostr << "X-ClickHouse-Summary: " << progress_string_writer.str() << "\r\n" << std::flush;
#endif
}

void WriteBufferFromHTTPServerResponse::writeHeaderProgress()
{
#if defined(POCO_CLICKHOUSE_PATCH)
    if (headers_finished_sending)
        return;

    WriteBufferFromOwnString progress_string_writer;
    accumulated_progress.writeJSON(progress_string_writer);

    if (response_header_ostr)
        *response_header_ostr << "X-ClickHouse-Progress: " << progress_string_writer.str() << "\r\n" << std::flush;
#endif
}

void WriteBufferFromHTTPServerResponse::finishSendHeaders()
{
    if (!headers_finished_sending)
    {
        writeHeaderSummary();
        headers_finished_sending = true;

        if (request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
        {
#if defined(POCO_CLICKHOUSE_PATCH)
            /// Send end of headers delimiter.
            if (response_header_ostr)
                *response_header_ostr << "\r\n" << std::flush;
#else
            /// Newline autosent by response.send()
            /// if nothing to send in body:
            if (!response_body_ostr)
                response_body_ostr = &(response.send());
#endif
        }
        else
        {
            if (!response_body_ostr)
                response_body_ostr = &(response.send());
        }
    }
}


void WriteBufferFromHTTPServerResponse::nextImpl()
{
    {
        std::lock_guard lock(mutex);

        startSendHeaders();

        if (!out && request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
        {
            if (compress)
            {
                auto content_encoding_name = toContentEncodingName(compression_method);

#if defined(POCO_CLICKHOUSE_PATCH)
                *response_header_ostr << "Content-Encoding: " << content_encoding_name << "\r\n";
#else
                response.set("Content-Encoding", content_encoding_name);
#endif
            }

#if !defined(POCO_CLICKHOUSE_PATCH)
            response_body_ostr = &(response.send());
#endif

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
    Poco::Net::HTTPServerRequest & request_,
    Poco::Net::HTTPServerResponse & response_,
    unsigned keep_alive_timeout_,
    bool compress_,
    CompressionMethod compression_method_)
    : BufferWithOwnMemory<WriteBuffer>(DBMS_DEFAULT_BUFFER_SIZE)
    , request(request_)
    , response(response_)
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
    if (offset())
    {
        next();

        if (out)
            out.reset();
    }
    else
    {
        /// If no remaining data, just send headers.
        std::lock_guard lock(mutex);
        startSendHeaders();
        finishSendHeaders();
    }
}


WriteBufferFromHTTPServerResponse::~WriteBufferFromHTTPServerResponse()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
