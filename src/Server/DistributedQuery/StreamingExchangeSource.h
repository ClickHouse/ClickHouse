#pragma once

#include <memory>
#include <Common/Epoll.h>
#include <Common/Logger.h>
#include <Common/WakeupFd.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Processors/ISource.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

class DistributedQueryCancellation;
using DistributedQueryCancellationPtr = std::shared_ptr<DistributedQueryCancellation>;

/// Reads one exchange stream from the producer task's `StreamingExchangeSink` over TCP. On the
/// initiator it reads the query result and shares the query's cancellation state: a lost producer is
/// recorded there and reported by the source driving the plan (see `tryGenerate`). On a worker the
/// state is null and a lost peer is a plain `EXCHANGE_PEER_DISCONNECTED` failure of the task.
class StreamingExchangeSource final : public ISource
{
public:
    explicit StreamingExchangeSource(
        SharedHeader header_,
        String query_id_,
        String stream_name_,
        String host_,
        UInt16 port_,
        DistributedQueryCancellationPtr cancellation_,
        String auth_token_ = {})
        : ISource(std::move(header_))
        , host(std::move(host_))
        , port(port_)
        , query_id(std::move(query_id_))
        , stream_name(std::move(stream_name_))
        , auth_token(std::move(auth_token_))
        , cancellation(std::move(cancellation_))
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        wait_events_epoll.add(output_update_wakeup.fd());
#endif
    }

    String getName() const override { return "StreamingExchangeSource(" + stream_name + ")"; }

    Status prepare() override;
    int schedule() override;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    std::tuple<int, uint32_t, Int64> scheduleForEvent() override;
#endif
    void onUpdatePorts() override;

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

    /// `readChunk`, unless the peer went away and the query reports that instead (see the class comment).
    std::optional<Chunk> tryGenerate() override;

    /// Read available data from the socket and deserialize a chunk when enough data was read.
    std::optional<Chunk> readChunk();

    /// Tell the sender that no more data is needed from it. Throws `EXCHANGE_PEER_DISCONNECTED` if the
    /// sender is gone.
    void sendNoMoreDataNeeded();

    const String host;
    const UInt16 port;
    const String query_id;
    const String stream_name;
    /// Auth token presented to the sink in SourceHello (empty when unauthenticated).
    const String auth_token;
    /// The query's shared cancellation state on the initiator; null on a worker (see the class comment).
    const DistributedQueryCancellationPtr cancellation;

    bool finished_reading = false;  /// All data has been read from socket.
    bool output_finished = false;   /// Output port is finished, do not need to receive more data.
    bool was_on_start_called = false;

    enum PacketReceiveState
    {
        ReceivingHeader,
        ReceivingBody,
    } packet_receive_state = ReceivingHeader;

    StreamingExchangeProtocol::PacketHeader current_packet_header{};
    size_t current_packet_header_bytes_filled = 0;

    std::vector<char> current_packet_body;
    size_t current_packet_body_bytes_filled = 0;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::unique_ptr<ReadBufferFromMemory> packet_in;    /// One full packet
    size_t rows_read = 0;
    size_t bytes_read = 0;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// Combines the socket and the output-update wakeup into one fd that the executor polls
    /// while the source waits in `Async`.
    Epoll wait_events_epoll;
#endif
    /// Written by `onUpdatePorts` (possibly from another thread) to wake the waiting source
    /// when its output port is updated - in particular closed by a satisfied `LIMIT`
    /// downstream; drained in `tryGenerate`.
    WakeupFd output_update_wakeup;

    LoggerPtr log = getLogger("StreamingExchangeSource");
};

}
