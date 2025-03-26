#pragma once

#include <Processors/ISource.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromPocoSocketChunked.h>

namespace DB
{

class StreamingExchangeSource final : public ISource
{
public:
    explicit StreamingExchangeSource(Block header_, Poco::Net::StreamSocket socket_, const String & stream_name_)
        : ISource(std::move(header_))
        , socket(socket_)
        , in(socket)
        , stream_name(stream_name_)
    {
    }

    String getName() const override { return "StreamingExchangeSource"; }

private:
    Chunk generate() override;

    Poco::Net::StreamSocket socket;
    ReadBufferFromPocoSocketChunked in;
    const String stream_name;
    LoggerPtr log = getLogger("StreamingExchangeSource");
};

}
