#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/DequeWithMemoryTracking.h>
#include <variant>
#include <Common/Epoll.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <optional>
#include <Common/Logger.h>
#include <Common/WakeupFd.h>
#include <Core/Types.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Poco/Net/StreamSocket.h>
#include <IO/WriteBufferFromString.h>
#include <Server/DistributedQuery/FutureConnection.h>

namespace DB
{

class StreamingExchangeSink final : public ISink
{
public:
    /// The input chunks are packets made by `StreamingExchangeSerializingTransform`, one per chunk,
    /// and are sent as they are. The sink adds the end-of-stream packet when the input ends.
    StreamingExchangeSink(SharedHeader header_, FutureConnectionPtr future_connection_, String stream_name_)
        : ISink(std::move(header_))
        , future_connection(std::move(future_connection_))
        , stream_name(std::move(stream_name_))
    {
        wait_events_epoll.add(port_update_wakeup.fd());
    }

    String getName() const override { return "StreamingExchangeSink(" + stream_name + ")"; }

    Status prepare() override;
    std::tuple<int, uint32_t, Int64> scheduleForEvent() override;
    void onUpdatePorts() override;

private:
    void consume(Chunk chunk) override;
    void onFinish() override;
    void work() override;

    /// Drain any inbound NoMoreDataNeeded packet or peer half-close. Safe to call at any time.
    void tryReceiveControlPacket();

    /// Non-blocking read into `buffer[position .. buffer_size]`, advancing `position`.
    /// Returns true on progress (including EAGAIN), false on peer half-close. Throws on hard errors.
    bool tryReadFromSocketNonBlocking(char * buffer, size_t buffer_size, size_t & position);

    /// Set `no_more_data_needed` and drop pending output buffers.
    void markNoMoreDataNeeded();

    /// Send the buffers of `send_queue` to the socket in non-blocking mode, in order.
    void sendToSocket();

    /// True while the unsent bytes are below the cap, so the sink may take another packet.
    bool canAddChunk() const;
    /// The status of a sink that must wait for room in its send queue; counts the wait.
    Status waitForSendQueueRoom();

    /// Writes the end-of-stream packet and sends it; nothing follows it on the stream.
    void sendEndOfStream();

    /// A buffer waiting to be sent: a packet column, shared with the sinks of the other destinations
    /// of a broadcast, or the end-of-stream packet.
    struct SendBuffer
    {
        std::variant<ColumnPtr, String> data;
        /// Packets in `data`, one; counted as sent once the whole buffer is written to the socket.
        size_t packets = 0;

        std::string_view bytes() const;
    };

    /// Append a ready buffer to `send_queue`.
    void enqueueBuffer(SendBuffer buffer);

    /// Extract socket from future connection
    void extractSocket();

    /// (Re)register the socket in `wait_events_epoll`: always listen for inbound control
    /// packets and errors; poll for writability only when there are unsent bytes, otherwise a
    /// writable idle socket would wake the executor in a busy loop.
    void updateSocketWaitEvents();

    bool hasUnsentBytes() const { return !send_queue.empty(); }

    FutureConnectionPtr future_connection;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    const String stream_name;

    /// Buffers in send order. The front buffer is being written to the socket, `send_position`
    /// bytes of it are sent.
    DequeWithMemoryTracking<SendBuffer> send_queue;
    size_t send_position = 0;
    /// Bytes in `send_queue` that are not sent yet.
    size_t send_queue_bytes = 0;

    size_t chunks_written = 0;
    size_t total_bytes_sent = 0;
    /// Runs while the sink waits for the receiving task to connect.
    std::optional<Stopwatch> connection_wait;
    /// Present while the sink takes no chunks because its send queue is full: the metric counts
    /// such sinks, the stopwatch feeds the profile event when the stall ends.
    struct SendQueueFull
    {
        Stopwatch since;
        CurrentMetrics::Increment sinks_metric;
    };
    std::optional<SendQueueFull> send_queue_full;

    /// Cap on the unsent bytes in `send_queue`; back-pressure trips here.
    static constexpr size_t MAX_PENDING_BYTES = 16 * 1024 * 1024;
    bool input_is_finished = false;     /// We have read all the data from input port.
    bool end_of_stream_added = false;   /// The end-of-stream packet is queued; nothing follows it.
    bool no_more_data_needed = false;   /// Set to true when exchange stream receiver has sent us NoMoreDataNeeded.

    /// Accumulator for the inbound NoMoreDataNeeded packet (single UInt64, no body).
    UInt64 incoming_packet_type = 0;
    size_t incoming_packet_bytes_filled = 0;

    /// Combines the socket and the port-update wakeup into one fd that the executor polls
    /// while the sink waits in `Async`.
    Epoll wait_events_epoll{EpollNesting::Leaf};
    /// Written by `onUpdatePorts` (possibly from another thread) to wake the waiting sink
    /// when its input port is updated; drained in `work`.
    WakeupFd port_update_wakeup;
    /// Events the socket is currently registered with in `wait_events_epoll`.
    uint32_t registered_socket_events = 0;

    LoggerPtr log = getLogger("StreamingExchangeSink");
};

}

#endif
