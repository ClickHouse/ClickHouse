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
        , rows_written(0)
    {
    }

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
    size_t rows_written;
    LoggerPtr log = getLogger("StreamingExchangeSink");
};

}
