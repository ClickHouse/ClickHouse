#include <IO/WriteBufferFromHTTPServerResponse.h>

#include <Poco/Version.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/Exception.h>
#include <IO/WriteBufferFromString.h>
#include <IO/HTTPCommon.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <IO/Progress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void WriteBufferFromHTTPServerResponse::startSendHeaders()
{
    if (!headers_started_sending)
    {
        headers_started_sending = true;

        if (add_cors_header)
            response.set("Access-Control-Allow-Origin", "*");

        setResponseDefaultHeaders(response, keep_alive_timeout);

#if POCO_CLICKHOUSE_PATCH
        if (request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
            std::tie(response_header_ostr, response_body_ostr) = response.beginSend();
#endif
    }
}


void WriteBufferFromHTTPServerResponse::finishSendHeaders()
{
    if (!headers_finished_sending)
    {
        headers_finished_sending = true;

        if (request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
        {
#if POCO_CLICKHOUSE_PATCH
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
        std::lock_guard<std::mutex> lock(mutex);

        startSendHeaders();

        if (!out && request.getMethod() != Poco::Net::HTTPRequest::HTTP_HEAD)
        {
            if (compress)
            {
                if (compression_method == ZlibCompressionMethod::Gzip)
                {
#if POCO_CLICKHOUSE_PATCH
                    *response_header_ostr << "Content-Encoding: gzip\r\n";
#else
                    response.set("Content-Encoding", "gzip");
#endif
                }
                else if (compression_method == ZlibCompressionMethod::Zlib)
                {
#if POCO_CLICKHOUSE_PATCH
                    *response_header_ostr << "Content-Encoding: deflate\r\n";
#else
                    response.set("Content-Encoding", "deflate");
#endif
                }
                else
                    throw Exception("Logical error: unknown compression method passed to WriteBufferFromHTTPServerResponse",
                                    ErrorCodes::LOGICAL_ERROR);
                /// Use memory allocated for the outer buffer in the buffer pointed to by out. This avoids extra allocation and copy.

#if !POCO_CLICKHOUSE_PATCH
                response_body_ostr = &(response.send());
#endif

                out_raw.emplace(*response_body_ostr);
                deflating_buf.emplace(*out_raw, compression_method, compression_level, working_buffer.size(), working_buffer.begin());
                out = &*deflating_buf;
            }
            else
            {
#if !POCO_CLICKHOUSE_PATCH
                response_body_ostr = &(response.send());
#endif

                out_raw.emplace(*response_body_ostr, working_buffer.size(), working_buffer.begin());
                out = &*out_raw;
            }
        }

        finishSendHeaders();

    }

    if (out)
    {
        out->position() = position();
        out->next();
    }
}


WriteBufferFromHTTPServerResponse::WriteBufferFromHTTPServerResponse(
    Poco::Net::HTTPServerRequest & request_,
    Poco::Net::HTTPServerResponse & response_,
    unsigned keep_alive_timeout_,
    bool compress_,
    ZlibCompressionMethod compression_method_,
    size_t size)
    : BufferWithOwnMemory<WriteBuffer>(size)
    , request(request_)
    , response(response_)
    , keep_alive_timeout(keep_alive_timeout_)
    , compress(compress_)
    , compression_method(compression_method_)
{
}


void WriteBufferFromHTTPServerResponse::onProgress(const Progress & progress)
{
    std::lock_guard<std::mutex> lock(mutex);

    /// Cannot add new headers if body was started to send.
    if (headers_finished_sending)
        return;

    accumulated_progress.incrementPiecewiseAtomically(progress);

    if (progress_watch.elapsed() >= send_progress_interval_ms * 1000000)
    {
        progress_watch.restart();

        /// Send all common headers before our special progress headers.
        startSendHeaders();

        WriteBufferFromOwnString progress_string_writer;
        accumulated_progress.writeJSON(progress_string_writer);

#if POCO_CLICKHOUSE_PATCH
        *response_header_ostr << "X-ClickHouse-Progress: " << progress_string_writer.str() << "\r\n" << std::flush;
#endif
    }
}


void WriteBufferFromHTTPServerResponse::finalize()
{
    if (offset())
    {
        next();
    }
    else
    {
        /// If no remaining data, just send headers.
        std::lock_guard<std::mutex> lock(mutex);
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
