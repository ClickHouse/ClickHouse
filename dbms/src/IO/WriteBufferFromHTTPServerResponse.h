#pragma once

#include <experimental/optional>
#include <mutex>
#include <Poco/Version.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/HTTPCommon.h>
#include <Common/NetException.h>
#include <Common/Stopwatch.h>
#include <Core/Progress.h>


namespace Poco
{
    namespace Net
    {
        class HTTPServerResponse;
    }
}


namespace DB
{

/// The difference from WriteBufferFromOStream is that this buffer gets the underlying std::ostream
/// (using response.send()) only after data is flushed for the first time. This is needed in HTTP
/// servers to change some HTTP headers (e.g. response code) before any data is sent to the client
/// (headers can't be changed after response.send() is called).
///
/// In short, it allows delaying the call to response.send().
///
/// Additionally, supports HTTP response compression (in this case corresponding Content-Encoding
/// header will be set).
///
/// Also this class write and flush special X-ClickHouse-Progress HTTP headers
///  if no data was sent at the time of progress notification.
/// This allows to implement progress bar in HTTP clients.
class WriteBufferFromHTTPServerResponse : public BufferWithOwnMemory<WriteBuffer>
{
private:
    Poco::Net::HTTPServerResponse & response;

    bool add_cors_header = false;
    bool compress = false;
    ZlibCompressionMethod compression_method;
    int compression_level = Z_DEFAULT_COMPRESSION;

    std::ostream * response_body_ostr = nullptr;

#if POCO_CLICKHOUSE_PATCH
    std::ostream * response_header_ostr = nullptr;
#endif

    std::experimental::optional<WriteBufferFromOStream> out_raw;
    std::experimental::optional<ZlibDeflatingWriteBuffer> deflating_buf;

    WriteBuffer * out = nullptr;     /// Uncompressed HTTP body is written to this buffer. Points to out_raw or possibly to deflating_buf.

    bool headers_started_sending = false;
    bool headers_finished_sending = false;    /// If true, you could not add any headers.

    Progress accumulated_progress;
    size_t send_progress_interval_ms = 100;
    Stopwatch progress_watch;

    std::mutex mutex;    /// progress callback could be called from different threads.


    /// Must be called under locked mutex.
    /// This method send headers, if this was not done already,
    ///  but not finish them with \r\n, allowing to send more headers subsequently.
    void startSendHeaders();

    /// This method finish headers with \r\n, allowing to start to send body.
    void finishSendHeaders();

    void nextImpl() override;

public:
    WriteBufferFromHTTPServerResponse(
        Poco::Net::HTTPServerResponse & response_,
        bool compress_ = false,        /// If true - set Content-Encoding header and compress the result.
        ZlibCompressionMethod compression_method_ = ZlibCompressionMethod::Gzip,
        size_t size = DBMS_DEFAULT_BUFFER_SIZE);

    /// Writes progess in repeating HTTP headers.
    void onProgress(const Progress & progress);

    /// Send at least HTTP headers if no data has been sent yet.
    /// Use after the data has possibly been sent and no error happened (and thus you do not plan
    /// to change response HTTP code.
    /// This method is idempotent.
    void finalize();

    /// Turn compression on or off.
    /// The setting has any effect only if HTTP headers haven't been sent yet.
    void setCompression(bool enable_compression)
    {
        compress = enable_compression;
    }

    /// Set compression level if the compression is turned on.
    /// The setting has any effect only if HTTP headers haven't been sent yet.
    void setCompressionLevel(int level)
    {
        compression_level = level;
    }

    /// Turn CORS on or off.
    /// The setting has any effect only if HTTP headers haven't been sent yet.
    void addHeaderCORS(bool enable_cors)
    {
        add_cors_header = enable_cors;
    }

    /// Don't send HTTP headers with progress more frequently.
    void setSendProgressInterval(size_t send_progress_interval_ms_)
    {
        send_progress_interval_ms = send_progress_interval_ms_;
    }

    ~WriteBufferFromHTTPServerResponse();
};

}
