#include <Common/logger_useful.h>

#include <IO/ReadBuffer.h>

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTP2/HTTP2Stream.h>

namespace DB
{

extern const size_t FRAME_HEADER_SIZE;

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace
{

class ReadBufferFromHTTP2Stream : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit ReadBufferFromHTTP2Stream(HTTP2Stream & stream_) : stream(stream_) {}

private:
    bool nextImpl() override
    {
        std::unique_lock lock(stream.input_mutex);

        if (stream.eof && stream.input.empty())
            return false;

        while (stream.input.empty() && !stream.eof && !stream.closed)
            stream.input_cv.wait(lock);

        if (stream.closed)
            throw Exception(ErrorCodes::NETWORK_ERROR, "HTTP/2 stream has been closed");
        if (stream.eof && stream.input.empty())
            return false;

        auto [t_memory, len] = std::move(stream.input.front());
        stream.input.pop_front();
        memory = std::move(t_memory);
        BufferBase::set(memory.data() + FRAME_HEADER_SIZE, len, 0);

        HTTP2StreamEvent event{.type=HTTP2StreamEventType::DATA_CONSUMED, .stream_id=stream.id, .payload=static_cast<uint32_t>(len)};
        stream.stream_event_pipe->writeBytes(&event, sizeof(event));

        return true;
    }

    HTTP2Stream & stream;
};

void sendErrorResponse(HTTP2ServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status)
{
    response.setStatusAndReason(status);
    response.makeStream()->finalize();
}

}

void HTTP2Stream::run()
{
    guard = shared_from_this();
    LoggerPtr log = getLogger("HTTP2Stream");
    try
    {
        std::unique_ptr<ReadBuffer> body = std::make_unique<ReadBufferFromHTTP2Stream>(*this);
        HTTPServerRequest server_request(std::move(request), std::move(body), true, client_address, server_address, secure, socket);
        response.attachRequest(&server_request);

        Poco::Timestamp now;

        if (!forwarded_for.empty())
            request.set("X-Forwarded-For", forwarded_for);

        if (secure)
        {
            size_t hsts_max_age = context->getMaxHstsAge();

            if (hsts_max_age > 0)
                response.add("Strict-Transport-Security", "max-age=" + std::to_string(hsts_max_age));
        }

        response.setDate(now);

        std::unique_ptr<HTTPRequestHandler> handler(factory->createRequestHandler(server_request));
        if (handler)
        {
            if (request.getExpectContinue() && response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
                response.send100Continue();
            handler->handleRequest(server_request, response);
        }
        else
            sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
    }
    catch (Poco::Exception & e)
    {
        LOG_ERROR(log, "Error in HTTP/2 stream: {}", e.what());
        if (!response.sendStarted())
        {
            try
            {
                sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }
        throw;
    }
    guard.reset();
}

}
