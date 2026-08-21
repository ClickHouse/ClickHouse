#pragma once

#ifdef OS_LINUX

#include <Common/Logger.h>
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
    StreamingExchangeSink(SharedHeader header_, FutureConnectionPtr future_connection_, String stream_name_)
        : ISink(std::move(header_))
        , future_connection(std::move(future_connection_))
        , stream_name(std::move(stream_name_))
    {
    }

    ~StreamingExchangeSink() override;

    String getName() const override { return "StreamingExchangeSink(" + stream_name + ")"; }

    Status prepare() override;
    std::pair<int, uint32_t> scheduleForEvent() override;

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

    /// Send data in current_send_buffer to socket in non-blocking mode.
    void sendToSocket();

    /// Checks if out buffer has not too much data already if so, it is possible to add new chunk.
    bool canAddChunk() const;

    /// Move out buffer to current_send_buffer and reset out. It is only possible if current_send_buffer have been fully sent to socket.
    /// Otherwise, need to wait on socket and then call this method again.
    void tryToSwitchSendBuffer();

    /// Extract socket from future connection
    void extractSocket();

    FutureConnectionPtr future_connection;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    const String stream_name;

    /// In-memory buffer to which the chunks are serialized.
    /// Once it becomes big enough we move it to current_send_buffer.
    std::shared_ptr<WriteBufferFromOwnString> out;

    /// This buffer is being written to socket
    String current_send_buffer;
    /// How many bytes were already written to socket
    size_t current_send_position_in_buffer = 0;

    size_t rows_written = 0;
    size_t total_bytes_sent = 0;

    const size_t FLUSH_BUFFER_TO_SOCKET_THRESHOLD = 128 * 1024;
    /// Cap on total unsent bytes (`current_send_buffer` + `out`); back-pressure trips here.
    static constexpr size_t MAX_PENDING_BYTES = 16 * 1024 * 1024;
    bool input_is_finished = false;     /// We have read all the data from input port.
    bool final_chunk_added = false;     /// Final empty chunk was added to signal the exchange stream receiver that we are done.
    bool no_more_data_needed = false;   /// Set to true when exchange stream receiver has sent us NoMoreDataNeeded.

    /// Accumulator for the inbound NoMoreDataNeeded packet (single UInt64, no body).
    UInt64 incoming_packet_type = 0;
    size_t incoming_packet_bytes_filled = 0;

    LoggerPtr log = getLogger("StreamingExchangeSink");
};

}

#endif
