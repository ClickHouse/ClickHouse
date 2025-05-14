#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <IO/CompressionMethod.h>
#include <IO/WriteHelpers.h>

#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Server/HTTP/HTTP2/HTTP2ServerResponse.h>
#include <Server/HTTP/HTTP2/HTTP2Stream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REQUIRED_PASSWORD;
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
    extern const int NETWORK_ERROR;
}

namespace
{

class WriteBufferFromHTTP2ServerResponse : public WriteBufferFromHTTPServerResponseBase
{
public:
    explicit WriteBufferFromHTTP2ServerResponse(HTTP2Stream & stream_, bool is_http_method_head_) : stream(stream_), is_http_method_head(is_http_method_head_) {}

    void sendBufferAndFinalize(const char * ptr, size_t size) override
    {
        write(ptr, size);
        finalize();
    }

    void setExceptionCode(int code) override
    {
        std::lock_guard lock(stream.output_mutex);

        stream.response.set("X-ClickHouse-Exception-Code", toString<int>(code));

        if (code == ErrorCodes::REQUIRED_PASSWORD)
        {
            stream.response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            stream.response.set("WWW-Authenticate", "Basic realm=\"ClickHouse server HTTP API\"");
        }
        else
            stream.response.setStatusAndReason(exceptionCodeToHTTPStatus(code));
    }

    bool cancelWithException(int exception_code_, const std::string & message, WriteBuffer * compression_buffer) noexcept override
    {
        bool use_compression_buffer = compression_buffer && !compression_buffer->isCanceled() && !compression_buffer->isFinalized();

        try
        {
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

            if (!stream.response.sendStarted())
                setExceptionCode(exception_code_);

            auto & out = use_compression_buffer ? *compression_buffer : *this;
            writeString(message, out);
            if (!message.ends_with('\n'))
                writeChar('\n', out);

            if (use_compression_buffer)
                compression_buffer->finalize();
            finalize();

            LOG_DEBUG(
                getLogger("WriteBufferFromHTTP2ServerResponse"),
                "Write buffer has been canceled with an error."
                " HTTP code: {}, error code: {}, message: <{}>,"
                " use compression: {}, data has been send through buffers: {}, compression discarded data: {}, discarded data: {}",
            stream.response.getStatus(), exception_code_, message,
            use_compression_buffer, data_sent, compression_discarded_data, discarded_data);
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

private:
    void nextImpl() override
    {
        notifyOnOutput(false);
    }

    void finalizeImpl() override
    {
        notifyOnOutput(true);
    }

    void notifyOnOutput(bool end_stream)
    {
        std::unique_lock lock(stream.output_mutex);

        if (stream.end_stream)
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "Cannot call next() or finalize() on finalized WriteBufferFromHTTP2ServerResponse");

        if (!stream.response_submitted)
        {
            if (add_cors_header)
                stream.response.set("Access-Control-Allow-Origin", "*");
            if (compression_method != CompressionMethod::None)
                stream.response.set("Content-Encoding", toContentEncodingName(compression_method));
        }

        stream.end_stream = end_stream;
        stream.output = is_http_method_head ? nullptr : &memory;
        stream.output_len = is_http_method_head ? 0 : offset();
        stream.output_consumed = 0;

        HTTP2StreamEvent event{.type=HTTP2StreamEventType::OUTPUT_READY, .stream_id=stream.id};
        stream.stream_event_pipe->writeBytes(&event, sizeof(event));

        while (stream.output_consumed != stream.output_len && !stream.closed)
            stream.output_cv.wait(lock);

        if (stream.closed)
            throw Exception(ErrorCodes::NETWORK_ERROR, "HTTP/2 stream has been closed");

        stream.output = nullptr;
        set(memory.data(), memory.size());
    }

    HTTP2Stream & stream;

    bool is_http_method_head;
};

}

void HTTP2ServerResponse::send100Continue()
{
    HTTP2StreamEvent event{HTTP2StreamEventType::SEND_100_CONTINUE, stream.id};
    stream.stream_event_pipe->writeBytes(&event, sizeof(event));
}

std::unique_ptr<WriteBufferFromHTTPServerResponseBase> HTTP2ServerResponse::makeUniqueStream()
{
    return std::make_unique<WriteBufferFromHTTP2ServerResponse>(stream, request && request->getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD);
}

}
