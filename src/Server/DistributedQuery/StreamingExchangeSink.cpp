#include <limits>
#include <memory>
#include <base/defines.h>
#ifdef OS_LINUX

#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/logger_useful.h>
#include <Poco/Net/NetException.h>

#include <sys/epoll.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

StreamingExchangeSink::~StreamingExchangeSink()
{
    if (out && !out->isFinalized())
        out->cancel();
}

void StreamingExchangeSink::extractSocket()
{
    if (socket)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Socket has already been extracted for exchange stream {}", stream_name);

    if (!future_connection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Future connection is not set for exchange stream {}", stream_name);

    if (!future_connection->isReady())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Future connection is expected be ready at this point. Wrong sequence of prepare/schedule/work calls for exchange stream {}", stream_name);

    LOG_TRACE(log, "Extracting socket from future connection for exchange stream {}", stream_name);
    socket = std::make_unique<Poco::Net::StreamSocket>(future_connection->getSocket());
    future_connection.reset();
    chassert(socket);

    /// Set socket to non-blocking mode after handshake is finished.
    socket->setBlocking(false);
    socket->setSendBufferSize(1 * 1024 * 1024);

    /// Prepare initial in-memory buffer for serializing chunks
    out = std::make_shared<WriteBufferFromOwnString>();
}

/// Send data to socket until the buffer is empty or until socket would block.
void StreamingExchangeSink::sendToSocket()
{
    /// Drain any inbound NoMoreDataNeeded / peer half-close before attempting to send.
    tryReceiveControlPacket();
    if (no_more_data_needed)
        return;

    while (current_send_position_in_buffer < current_send_buffer.size())
    {
        try
        {
            /// `markNoMoreDataNeeded` clears `current_send_buffer`, so we can't be in this loop.
            chassert(!no_more_data_needed);

            size_t bytes_to_send = current_send_buffer.size() - current_send_position_in_buffer;
            /// Saturate at INT_MAX: a plain cast would wrap negative for buffers > 2 GiB, after
            /// which Poco's wrapper short-circuits without ever calling ::send.
            ssize_t sent = socket->sendBytes(
                current_send_buffer.data() + current_send_position_in_buffer,
                static_cast<int>(std::min<size_t>(bytes_to_send, std::numeric_limits<int>::max())));
            if (sent < 0)
            {
                auto last_error = errno;
                if (last_error == EINTR)
                {
                    continue;
                }
                else if (last_error == EAGAIN || last_error == EWOULDBLOCK)
                {
                    /// Socket is not ready for writing, wait for epoll event
                    break;
                }
                else
                {
                    throw Poco::Net::NetException(fmt::format("Failed to send data to socket for stream {}, last error {}", stream_name, last_error));
                }
            }

            LOG_TEST(log, "Sent {} bytes to exchange stream {}, fd: {}", sent, stream_name, socket->sockfd());

            current_send_position_in_buffer += sent;
            total_bytes_sent += sent;
        }
        catch (const Poco::IOException & e)
        {
            /// Peer may have sent NoMoreDataNeeded or half-closed; only swallow the exception
            /// in those cases, otherwise it's a real network error.
            LOG_TRACE(log, "Send to exchange stream {} hit IO exception: {}; checking for peer close", stream_name, e.displayText());
            tryReceiveControlPacket();
            if (!no_more_data_needed)
                throw;
            return;
        }
    }

    /// Is there enough serialized data to start sending it to socket?
    if (out->count() >= FLUSH_BUFFER_TO_SOCKET_THRESHOLD)
        tryToSwitchSendBuffer();
}

bool StreamingExchangeSink::canAddChunk() const
{
    const size_t unsent = current_send_buffer.size() - current_send_position_in_buffer;
    return (unsent + out->count()) < MAX_PENDING_BYTES;
}

void StreamingExchangeSink::tryToSwitchSendBuffer()
{
    /// Check that current_send_buffer has been fully sent to socket
    if (current_send_position_in_buffer < current_send_buffer.size())
        return;

    /// Check that new buffer has anything in it
    if (out->count() == 0)
        return;

    out->finalize();
    current_send_buffer = out->str();
    current_send_position_in_buffer = 0;
    out = std::make_shared<WriteBufferFromOwnString>();
}

ISink::Status StreamingExchangeSink::prepare()
{
    /// If socket is not ready yet, wait for it
    if (!socket)
        return Status::Async;

    if (has_input)
        return canAddChunk() ? Status::Ready : Status::Async;

    if (input.isFinished())
    {
        if (!final_chunk_added)
        {
            if (!canAddChunk())
                return Status::Async;
            /// Input is finished, send an empty chunk to signal end-of-stream.
            input_is_finished = true;
            current_chunk = {};
            has_input = true;
            return Status::Ready;
        }

        /// Need ot flush all remaining data
        if (current_send_position_in_buffer < current_send_buffer.size() || out->count() > 0)
            return Status::Async;

        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    /// Propagate back-pressure upstream: don't pull until there's room.
    if (!canAddChunk())
        return Status::Async;

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void StreamingExchangeSink::work()
{
    /// Try to extract socket if not done yet
    if (!socket)
    {
        extractSocket();
        return;
    }

    /// React to EPOLLIN / EPOLLRDHUP wakeups (see scheduleForEvent).
    tryReceiveControlPacket();

    if (has_input)
    {
        /// If we have already added final chunk then no new input is expected
        chassert(!final_chunk_added);

        has_input = false;
        if (input_is_finished)
        {
            /// Send empty final chunk
            chassert(!current_chunk);
            final_chunk_added = true;
            consume(std::move(current_chunk));
        }
        else if (current_chunk)
        {
            /// It the chunk is not the final, send it only if it is not empty
            consume(std::move(current_chunk));
        }

        return;
    }

    /// Send pending data to socket
    if (current_send_position_in_buffer < current_send_buffer.size() || out->count() > 0)
    {
        sendToSocket();
        tryToSwitchSendBuffer();
        return;
    }

    if (!was_on_finish_called)
    {
        was_on_finish_called = true;
        onFinish();
        return;
    }
}

std::pair<int, uint32_t> StreamingExchangeSink::scheduleForEvent()
{
    /// If socket is not ready yet, wait on the eventfd
    if (!socket)
    {
        if (!future_connection)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Future connection is not set for exchange stream {}", stream_name);

        if (future_connection->isReady())
            extractSocket();
    }

    if (socket)
    {
        LOG_TEST(log, "Schedule exchange stream sink {}, socket is ready, fd: {}", stream_name, socket->sockfd());
        /// EPOLLIN | EPOLLRDHUP wake us on peer-initiated NoMoreDataNeeded / half-close.
        return {
            socket->sockfd(),
            EPOLLOUT | EPOLLIN | EPOLLRDHUP | EPOLLERR};
    }

    int fd = future_connection->getEventFd();

    LOG_TEST(log, "Schedule exchange stream sink {} waiting for connection, eventfd: {}", stream_name, fd);
    return {fd, EPOLLIN | EPOLLERR};
}

void StreamingExchangeSink::consume(Chunk chunk)
{
    if (no_more_data_needed)
    {
        /// We have to consume all chunks from input even if we have already received NoMoreDataNeeded packet.
        /// This is needed to avoid stuck pipeline in case of some buckets of ShuffleExchange don't need data while others still do.
        /// So we just drop the chunk and continue.
        /// TODO: is there a better way to figure out when when all buckets don't need data and close the inputs in pipeline?
        LOG_TEST(log, "No more data needed for exchange stream {}, dropping chunk with {} rows", stream_name, chunk.getNumRows());
        return;
    }

    rows_written += chunk.getNumRows();

    if (chunk.getNumRows() == 0 && chunk.getNumColumns() != 0)
    {
        LOG_TEST(log, "Unexpected chunk with 0 rows to exchange stream {}", stream_name);
    }

    LOG_TEST(log, "Writing chunk with {} rows to exchange stream {}", chunk.getNumRows(), stream_name);

    /// Write packet header stub.
    /// The actual size will be calculated and overwritten after the chuck is serialized
    const ssize_t packet_header_offset = out->count();
    StreamingExchangeProtocol::PacketHeader packet_header{.packet_type = StreamingExchangeProtocol::PacketType::Data, .bytes_size = 0};
    out->write(reinterpret_cast<const char*>(&packet_header), sizeof(packet_header));

    const bool final_chunk = chunk.empty();
    auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
    const bool has_aggregated_chunk_info = !!agg_info;
    UInt64 flags = 0;
    if (final_chunk)
        flags |= 1;
    if (has_aggregated_chunk_info)
        flags |= 2;
    writeVarUInt(flags, *out);
    writeVarUInt(chunk.getNumRows(), *out);
    writeVarUInt(chunk.getNumColumns(), *out);
    /// chunk_num has no BlockInfo field; carry it in the exchange framing so memory-bound merging
    /// can restore chunk order on the receiver.
    if (has_aggregated_chunk_info)
        writeVarUInt(agg_info->chunk_num, *out);

    if (chunk.getNumColumns() > 0)
    {
        auto compressed_buf = std::make_unique<CompressedWriteBuffer>(*out);
        auto writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_TCP_PROTOCOL_VERSION, input.getSharedHeader());

        Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
        /// Carry the remaining aggregation metadata in block.info, the same way partial-aggregation
        /// results are transported for distributed/parallel reads.
        if (agg_info)
        {
            block.info.bucket_num = agg_info->bucket_num;
            block.info.is_overflows = agg_info->is_overflows;
            block.info.out_of_order_buckets = agg_info->out_of_order_buckets;
        }
        writer->write(block);

        writer->flush();
        compressed_buf->finalize();
    }

    /// Fill the actual size in the header
    {
        /// `out` is a WriteBufferFromString to we can rely on count() for getting curretn position in the buffer.
        const ssize_t end_of_packet_offset = out->count();
        const ssize_t packet_data_size = end_of_packet_offset - packet_header_offset - sizeof(StreamingExchangeProtocol::PacketHeader);

        if (packet_data_size < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid packet data size: {}", packet_data_size);

        /// The receiver rejects Data packets above this limit; fail here with a clear, local error
        /// instead of sending one the peer would reject. Splitting large chunks is not implemented yet.
        if (static_cast<UInt64>(packet_data_size) > StreamingExchangeProtocol::MAX_DATA_PACKET_BODY_BYTES)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Exchange data packet of {} bytes exceeds the maximum {}; splitting large chunks is not implemented",
                packet_data_size, StreamingExchangeProtocol::MAX_DATA_PACKET_BODY_BYTES);

        /// Fill bytes_size field using memcpy because packet header address in the buffer might not be properly aligned.
        char * packet_header_start = const_cast<char*>(out->stringView().data()) + packet_header_offset;
        static_assert(sizeof(StreamingExchangeProtocol::PacketHeader::bytes_size) == sizeof(packet_data_size));
        memcpy(packet_header_start + offsetof(StreamingExchangeProtocol::PacketHeader, bytes_size), &packet_data_size, sizeof(packet_data_size));

        LOG_TEST(log, "Packet with {} bytes was added to exchange stream {}", packet_data_size, stream_name);
    }

    if (chunk.getNumRows() == 0)
    {
        /// Just in case, flush buffer to the socket
        tryToSwitchSendBuffer();
    }

    sendToSocket();
}

void StreamingExchangeSink::onFinish()
{
    LOG_TRACE(log, "Finished writing to exchange stream {}, total rows: {}, bytes: {}",
        stream_name, rows_written, total_bytes_sent);
}

bool StreamingExchangeSink::tryReadFromSocketNonBlocking(char * buffer, size_t buffer_size, size_t & position)
{
    while (position < buffer_size)
    {
        const size_t remaining = buffer_size - position;
        ssize_t received = socket->receiveBytes(
            buffer + position,
            static_cast<int>(std::min<size_t>(remaining, std::numeric_limits<int>::max())));
        if (received < 0)
        {
            auto last_error = errno;
            if (last_error == EINTR)
                continue;
            if (last_error == EAGAIN || last_error == EWOULDBLOCK)
                return true; /// No data right now, try later.
            throw Poco::Net::NetException(fmt::format(
                "Failed to receive control packet on exchange stream {}, error {}", stream_name, last_error));
        }
        if (received == 0)
            return false; /// Peer half-closed.
        position += received;
    }
    return true;
}

void StreamingExchangeSink::tryReceiveControlPacket()
{
    if (no_more_data_needed)
        return;
    if (!socket)
        return;

    const bool not_eof = tryReadFromSocketNonBlocking(
        reinterpret_cast<char *>(&incoming_packet_type),
        sizeof(incoming_packet_type),
        incoming_packet_bytes_filled);

    if (incoming_packet_bytes_filled == sizeof(incoming_packet_type))
    {
        if (incoming_packet_type != StreamingExchangeProtocol::PacketType::NoMoreDataNeeded)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Unexpected packet type {} from peer on exchange stream {}",
                incoming_packet_type, stream_name);

        LOG_TRACE(log, "Received NoMoreDataNeeded for exchange stream {}", stream_name);
        markNoMoreDataNeeded();
        return;
    }

    if (!not_eof)
    {
        if (incoming_packet_bytes_filled > 0)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Peer half-closed exchange stream {} after {} of {} control bytes; truncated control message",
                stream_name, incoming_packet_bytes_filled, sizeof(incoming_packet_type));

        /// Normal end-of-stream: after the sink sent the final empty chunk, the source consumes it
        /// and closes without sending NoMoreDataNeeded. Treat EOF as benign only with nothing left to send.
        const size_t unsent = current_send_buffer.size() - current_send_position_in_buffer;
        if (!final_chunk_added || unsent > 0 || out->count() > 0)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Peer half-closed exchange stream {} without sending NoMoreDataNeeded "
                "(final_chunk_added={}, unsent={}, out={})",
                stream_name, final_chunk_added, unsent, out->count());

        LOG_TRACE(log, "Peer closed exchange stream {} after consuming the final chunk", stream_name);
        markNoMoreDataNeeded();
    }
}

void StreamingExchangeSink::markNoMoreDataNeeded()
{
    no_more_data_needed = true;
    /// Drop pending output: consume() drops new chunks too, no more sends will happen.
    current_send_buffer.clear();
    current_send_position_in_buffer = 0;
    out = std::make_shared<WriteBufferFromOwnString>();
}

}

#endif
