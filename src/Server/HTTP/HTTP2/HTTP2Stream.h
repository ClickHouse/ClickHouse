#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>

#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPRequest.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPResponse.h>
#include <Server/HTTP/HTTP2/HTTP2ServerResponse.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/SocketImpl.h>
#include <Poco/Pipe.h>
#include <Poco/Runnable.h>

#include <condition_variable>
#include <deque>
#include <memory>

namespace DB
{

enum class HTTP2StreamEventType : uint32_t
{
    OUTPUT_READY,
    SEND_100_CONTINUE,
    DATA_CONSUMED,
};

struct HTTP2StreamEvent
{
    HTTP2StreamEventType type;
    uint32_t stream_id;
    uint32_t payload = 0;
};

class HTTP2Stream : public Poco::Runnable, public std::enable_shared_from_this<HTTP2Stream>
{
public:
    explicit HTTP2Stream(
        size_t id_,
        std::shared_ptr<Poco::Pipe> stream_event_pipe_,
        HTTPContextPtr context_,
        HTTPRequestHandlerFactoryPtr factory_,
        std::string forwarded_for_,
        const Poco::Net::SocketAddress & client_address_,
        const Poco::Net::SocketAddress & server_address_,
        bool secure_,
        Poco::Net::SocketImpl * socket_,
        std::function<void()> on_finish_)
         : id(id_), context(context_), factory(factory_), forwarded_for(std::move(forwarded_for_))
         , client_address(client_address_), server_address(server_address_), secure(secure_), socket(socket_)
         , response(*this), stream_event_pipe(stream_event_pipe_), on_finish(std::move(on_finish_))
    {
    }

    ~HTTP2Stream() override
    {
        on_finish();
    }

private:
    void run() override;

public:
    std::shared_ptr<HTTP2Stream> guard;

    const uint32_t id;
    HTTPContextPtr context;
    HTTPRequestHandlerFactoryPtr factory;
    std::string forwarded_for;
    Poco::Net::SocketAddress client_address;
    Poco::Net::SocketAddress server_address;
    bool secure;
    Poco::Net::SocketImpl * socket;   /// FIXME: should not be here

    HTTPRequest request;   /// Moved out in run()

    std::atomic<bool> closed = false;

    std::atomic<bool> eof = false;
    std::deque<std::pair<Memory<>, size_t>> input;
    std::mutex input_mutex;
    std::condition_variable input_cv;

    HTTP2ServerResponse response;
    bool response_submitted = false;
    bool output_deferred = false;
    bool end_stream = false;
    std::deque<std::pair<Memory<>, size_t>> output;
    std::pair<Memory<>, size_t> cur_output = {{}, 0};
    size_t cur_output_consumed = 0;
    std::mutex output_mutex;
    std::condition_variable output_cv;

    std::shared_ptr<Poco::Pipe> stream_event_pipe;

    std::function<void()> on_finish;
};

}
