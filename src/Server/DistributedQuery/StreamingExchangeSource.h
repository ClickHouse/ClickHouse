#pragma once

#include <Processors/ISource.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

namespace DB
{

class StreamingExchangeSource final : public ISource
{
public:
    explicit StreamingExchangeSource(Block header_, Poco::Net::StreamSocket socket_, const String & stream_name_)
        : ISource(std::move(header_))
        , socket(socket_)
        , in(socket)
        , out(socket)
        , stream_name(stream_name_)
    {
        socket.setReceiveBufferSize(10 * 1024 * 1024);
    }

    ~StreamingExchangeSource() override;

    String getName() const override { return "StreamingExchangeSource(" + stream_name + ")"; }

    Status prepare() override;
    int schedule() override;

private:
    Chunk generate() override;

    void sendNoMoreDataNeeded();

    bool finished_reading = false;  /// All data has been read from socket.
    bool output_finished = false;   /// Output port is finished, do not need to receive more data.
    Poco::Net::StreamSocket socket;
    ReadBufferFromPocoSocket in;
    WriteBufferFromPocoSocket out;
    const String stream_name;
    size_t rows_read = 0;
    LoggerPtr log = getLogger("StreamingExchangeSource");
};

}
