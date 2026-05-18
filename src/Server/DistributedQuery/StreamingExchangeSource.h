#pragma once

#include <memory>
#include <Common/Logger.h>
#include <IO/ReadBufferFromPocoSocket.h>
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
    explicit StreamingExchangeSource(SharedHeader header_, String query_id_, String stream_name_, String host_, UInt16 port_)
        : ISource(std::move(header_))
        , host(std::move(host_))
        , port(port_)
        , query_id(std::move(query_id_))
        , stream_name(std::move(stream_name_))
    {
    }

    ~StreamingExchangeSource() override;

    String getName() const override { return "StreamingExchangeSource(" + stream_name + ")"; }

    Status prepare() override;
    int schedule() override;

private:
    void onStart();
    void connect();
    void sendHello();
    void receiveHello();

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

    const String host;
    const UInt16 port;
    const String query_id;
    const String stream_name;

    bool finished_reading = false;  /// All data has been read from socket.
    bool output_finished = false;   /// Output port is finished, do not need to receive more data.
    bool was_on_start_called = false;

    enum PacketReceiveState
    {
        ReceivingHeader,
        ReceivingBody,
    } packet_receive_state = ReceivingHeader;

    StreamingExchangeProtocol::PacketHeader current_packet_header;
    size_t current_packet_header_bytes_filled = 0;

    std::vector<char> current_packet_body;
    size_t current_packet_body_bytes_filled = 0;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::unique_ptr<ReadBufferFromMemory> packet_in;    /// One full packet
    std::unique_ptr<WriteBufferFromPocoSocket> out;
    size_t rows_read = 0;
    size_t bytes_read = 0;
    LoggerPtr log = getLogger("StreamingExchangeSource");
};

}
