#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include "HTTPOutputStreams.h"


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

HTTPOutputStreams::HTTPOutputStreams(HTTPResponseBufferPtr & raw_out, bool internal_compress)
    : out(raw_out)
    , out_maybe_compressed(createMaybeCompressionOut(internal_compress, out))
    , out_maybe_delayed_and_compressed(out_maybe_compressed)
{
}

HTTPOutputStreams::HTTPOutputStreams(HTTPResponseBufferPtr & raw_out, Context & context, HTTPServerRequest & request, HTMLForm & form)
    : out(raw_out)
    , out_maybe_compressed(createMaybeCompressionOut(form.getParsed<bool>("compress", false), out))
    , out_maybe_delayed_and_compressed(createMaybeDelayedAndCompressionOut(context, form, out_maybe_compressed))
{
    Settings & settings = context.getSettingsRef();

    /// HTTP response compression is turned on only if the client signalled that they support it
    /// (using Accept-Encoding header) and 'enable_http_compression' setting is turned on.
    out->setCompression(out->getCompression() && settings.enable_http_compression);
    if (out->getCompression())
        out->setCompressionLevel(settings.http_zlib_compression_level);

    out->setSendProgressInterval(settings.http_headers_progress_interval_ms);

    /// Add CORS header if 'add_http_cors_header' setting is turned on and the client passed Origin header.
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

WriteBufferPtr HTTPOutputStreams::createMaybeCompressionOut(bool compression, HTTPResponseBufferPtr & out_)
{
    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    return compression ? std::make_shared<CompressedWriteBuffer>(*out_) : WriteBufferPtr(out_);
}

WriteBufferPtr HTTPOutputStreams::createMaybeDelayedAndCompressionOut(Context & context, HTMLForm & form, WriteBufferPtr & out_)
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
            auto push_memory_buffer_and_continue = [next_buffer = out_] (const WriteBufferPtr & prev_buf)
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

    return out_;
}

HTTPOutputStreams::~HTTPOutputStreams()
{
    /// This could be a broken HTTP Request
    /// Because it does not call finalize or writes some data to output stream after call finalize
    /// In this case we need to clean up its broken state to ensure that they are not sent to the client

    /// For delayed stream, we destory CascadeBuffer and without sending any data to client.
    if (out_maybe_delayed_and_compressed != out_maybe_compressed)
        out_maybe_delayed_and_compressed.reset();

    if (out->count() == out->offset())
    {
        /// If buffer has data and server never sends data to client
        /// no need to send that data
        out_maybe_compressed->position() = out_maybe_compressed->buffer().begin();
        out->position() = out->buffer().begin();
    }
}

void HTTPOutputStreams::finalize() const
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

    /// Send HTTP headers with code 200 if no exception happened and the data is still not sent to the client.
    out_maybe_compressed->next();
    out->next();
    out->finalize();
}

}
