#pragma once

#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/WriteBufferFromPocoSocketChunked.h>

namespace DB
{

class StreamingExchangeSink final : public ISink
{
public:
    StreamingExchangeSink(Block header_, String query_id_, String stream_name_, String host_, UInt16 port_)
        : ISink(std::move(header_))
        , host(std::move(host_))
        , port(port_)
        , query_id(std::move(query_id_))
        , stream_name(std::move(stream_name_))
    {
    }

    ~StreamingExchangeSink() override;

    String getName() const override { return "StreamingExchangeSink(" + stream_name + ")"; }

private:
    void onStart() override;

    void consume(Chunk chunk) override;

    void onFinish() override;

    void connect();
    void sendHello();
    void receiveHello();

    const String host;
    const UInt16 port;
    const String query_id;
    const String stream_name;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBufferFromPocoSocketChunked> in;
    std::shared_ptr<WriteBufferFromPocoSocketChunked> out;
    size_t rows_written = 0;

    /// Keep track of out->count() when the last call to out->next() was made
    /// and not call out->next() if the difference is not big enough to avoid doing send() for small chaunks
    size_t bytes_sent_to_socket = 0;
    const size_t bytes_sent_to_socket_threshold = 128 * 1024;
    /// After calling out->next() we will return Async from prepare() to epoll for out socket becoming ready
    bool wait_for_out_socket_ready = true;

    LoggerPtr log = getLogger("StreamingExchangeSink");
};

}
