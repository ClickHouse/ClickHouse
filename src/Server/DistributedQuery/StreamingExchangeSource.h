#pragma once

#include <memory>
#include <Processors/ISource.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromPocoSocket.h>

namespace DB
{

class StreamingExchangeSource final : public ISource
{
public:
    explicit StreamingExchangeSource(Block header_, Poco::Net::StreamSocket socket_, const String & stream_name_)
        : ISource(std::move(header_))
        , socket(socket_)
        , out(socket)
        , stream_name(stream_name_)
    {
        socket.setReceiveBufferSize(10 * 1024 * 1024);
        socket.setBlocking(false);
        packet_receive_state = ReceivingHeader;
        current_packet_header_bytes_filled = 0;
    }

    ~StreamingExchangeSource() override;

    String getName() const override { return "StreamingExchangeSource(" + stream_name + ")"; }

    Status prepare() override;
    int schedule() override;

private:
    /// Read as many bytes as we can from the socket without blocking and update position accordingly.
    void readFromSocket(char * buffer, size_t buffer_size, size_t & position);

    /// Continue reading packet header until it is fully read. Then we know the full size and can start reading the body.
    void tryReadHeader();
    /// Continue reading packet body until it is fully read.
    void tryReadBody();

    /// Read available data from the socket and deserialize a chunk when enough data was read.
    std::optional<Chunk> tryGenerate() override;

    /// Tell the sender that no more data is needed from it.
    void sendNoMoreDataNeeded();

    bool finished_reading = false;  /// All data has been read from socket.
    bool output_finished = false;   /// Output port is finished, do not need to receive more data.

    enum PacketReceiveState
    {
        ReceivingHeader,
        ReceivingBody,
    } packet_receive_state = ReceivingHeader;

    StreamingExchangeProtocol::DataPacketHeader current_packet_header;
    size_t current_packet_header_bytes_filled = 0;

    std::vector<char> current_packet_body;
    size_t current_packet_body_bytes_filled = 0;

    Poco::Net::StreamSocket socket;
    std::unique_ptr<ReadBufferFromMemory> packet_in;    /// One full packet
    WriteBufferFromPocoSocket out;
    const String stream_name;
    size_t rows_read = 0;
    size_t bytes_read = 0;
    LoggerPtr log = getLogger("StreamingExchangeSource");
};

}
