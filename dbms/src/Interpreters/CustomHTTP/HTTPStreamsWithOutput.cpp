#include <Interpreters/CustomHTTP/HTTPStreamsWithOutput.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ConcatReadBuffer.h>


namespace DB
{

namespace
{
    inline void listeningProgress(Context & context, ProgressCallback listener)
    {
        auto prev = context.getProgressCallback();
        context.setProgressCallback([prev, listener] (const Progress & progress)
        {
            if (prev)
                prev(progress);

            listener(progress);
        });
    }

    inline ProgressCallback cancelListener(Context & context, Poco::Net::StreamSocket & socket)
    {
        /// Assume that at the point this method is called no one is reading data from the socket any more.
        /// True for read-only queries.
        return [&context, &socket](const Progress &)
        {
            try
            {
                char b;
                int status = socket.receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK);
                if (status == 0)
                    context.killCurrentQuery();
            }
            catch (Poco::TimeoutException &)
            {
            }
            catch (...)
            {
                context.killCurrentQuery();
            }
        };
    }
}

void HTTPStreamsWithOutput::attachSettings(Context & context, Settings & settings, HTTPServerRequest & request)
{
    /// HTTP response compression is turned on only if the client signalled that they support it
    /// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
    out->setCompression(out->getCompression() && settings.enable_http_compression);
    if (out->getCompression())
        out->setCompressionLevel(settings.http_zlib_compression_level);

    out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

    /// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed
    /// Origin header.
    out->addHeaderCORS(settings.add_http_cors_header && !request.get("Origin", "").empty());

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    if (settings.send_progress_in_http_headers)
        listeningProgress(context, [this] (const Progress & progress) { out->onProgress(progress); });

    if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
    {
        Poco::Net::StreamSocket & socket = dynamic_cast<Poco::Net::HTTPServerRequestImpl &>(request).socket();

        listeningProgress(context, cancelListener(context, socket));
    }
}

void HTTPStreamsWithOutput::attachRequestAndResponse(
    Context & context, HTTPServerRequest & request, HTTPServerResponse & response, HTMLForm & form, size_t keep_alive_timeout)
{
    out = createEndpoint(request, response, keep_alive_timeout);
    out_maybe_compressed = createMaybeCompressionEndpoint(form, out);
    out_maybe_delayed_and_compressed = createMaybeDelayedAndCompressionEndpoint(context, form, out_maybe_compressed);
}

std::shared_ptr<WriteBufferFromHTTPServerResponse> HTTPStreamsWithOutput::createEndpoint(
    HTTPServerRequest & request, HTTPServerResponse & response, size_t keep_alive_timeout)
{
    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");

    if (!http_response_compression_methods.empty())
    {
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.
        if (std::string::npos != http_response_compression_methods.find("gzip"))
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive_timeout, true, CompressionMethod::Gzip, DBMS_DEFAULT_BUFFER_SIZE);
        else if (std::string::npos != http_response_compression_methods.find("deflate"))
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive_timeout, true, CompressionMethod::Zlib, DBMS_DEFAULT_BUFFER_SIZE);
#if USE_BROTLI
        else if (http_response_compression_methods == "br")
            return std::make_shared<WriteBufferFromHTTPServerResponse>(
                request, response, keep_alive_timeout, true, CompressionMethod::Brotli, DBMS_DEFAULT_BUFFER_SIZE);
#endif
    }

    return std::make_shared<WriteBufferFromHTTPServerResponse>(
        request, response, keep_alive_timeout, false, CompressionMethod{}, DBMS_DEFAULT_BUFFER_SIZE);
}

WriteBufferPtr HTTPStreamsWithOutput::createMaybeCompressionEndpoint(HTMLForm & form, std::shared_ptr<WriteBufferFromHTTPServerResponse> & endpoint)
{
    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    bool internal_compression = form.getParsed<bool>("compress", false);
    return internal_compression ? std::make_shared<CompressedWriteBuffer>(*endpoint) : WriteBufferPtr(endpoint);
}

WriteBufferPtr HTTPStreamsWithOutput::createMaybeDelayedAndCompressionEndpoint(Context & context, HTMLForm & form, WriteBufferPtr & endpoint)
{
    /// If it is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    bool buffer_until_eof = form.getParsed<bool>("wait_end_of_query", false);

    /// At least, we should postpone sending of first buffer_size result bytes
    size_t buffer_size_total = std::max(form.getParsed<size_t>("buffer_size", DBMS_DEFAULT_BUFFER_SIZE), static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE));

    size_t buffer_size_memory = (buffer_size_total > DBMS_DEFAULT_BUFFER_SIZE) ? buffer_size_total : 0;

    if (buffer_size_memory > 0 || buffer_until_eof)
    {
        CascadeWriteBuffer::WriteBufferPtrs cascade_buffer1;
        CascadeWriteBuffer::WriteBufferConstructors cascade_buffer2;

        if (buffer_size_memory > 0)
            cascade_buffer1.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_memory));

        if (buffer_until_eof)
        {
            std::string tmp_path_template = context.getTemporaryPath() + "http_buffers/";

            auto create_tmp_disk_buffer = [tmp_path_template] (const WriteBufferPtr &)
            {
                return WriteBufferFromTemporaryFile::create(tmp_path_template);
            };

            cascade_buffer2.emplace_back(std::move(create_tmp_disk_buffer));
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = endpoint] (const WriteBufferPtr & prev_buf)
            {
                auto prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception("Expected MemoryWriteBuffer", ErrorCodes::LOGICAL_ERROR);

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf , *next_buffer);

                return next_buffer;
            };

            cascade_buffer2.emplace_back(push_memory_buffer_and_continue);
        }

        return std::make_shared<CascadeWriteBuffer>(std::move(cascade_buffer1), std::move(cascade_buffer2));
    }

    return endpoint;
}

void HTTPStreamsWithOutput::finalize() const
{
    if (out_maybe_delayed_and_compressed != out_maybe_compressed)
    {
        /// TODO: set Content-Length if possible
        std::vector<WriteBufferPtr> write_buffers;
        std::vector<ReadBufferPtr> read_buffers;
        std::vector<ReadBuffer *> read_buffers_raw_ptr;

        auto cascade_buffer = typeid_cast<CascadeWriteBuffer *>(out_maybe_delayed_and_compressed.get());
        if (!cascade_buffer)
            throw Exception("Expected CascadeWriteBuffer", ErrorCodes::LOGICAL_ERROR);

        cascade_buffer->getResultBuffers(write_buffers);

        if (write_buffers.empty())
            throw Exception("At least one buffer is expected to overwrite result into HTTP response", ErrorCodes::LOGICAL_ERROR);

        for (auto & write_buf : write_buffers)
        {
            IReadableWriteBuffer * write_buf_concrete;
            ReadBufferPtr reread_buf;

            if (write_buf
                && (write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
                && (reread_buf = write_buf_concrete->tryGetReadBuffer()))
            {
                read_buffers.emplace_back(reread_buf);
                read_buffers_raw_ptr.emplace_back(reread_buf.get());
            }
        }

        ConcatReadBuffer concat_read_buffer(read_buffers_raw_ptr);
        copyData(concat_read_buffer, *out_maybe_compressed);
    }

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to
    /// the client.
    out->finalize();
}

}
