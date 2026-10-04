#include <limits>
#include <memory>
#include <base/defines.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Columns/IColumn.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/Epoll.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/Net/NetException.h>

#include <unistd.h>


namespace ProfileEvents
{
    extern const Event StreamingExchangeSendBytes;
    extern const Event StreamingExchangePacketsSent;
    extern const Event StreamingExchangeSendQueueFullMicroseconds;
    extern const Event StreamingExchangeConnectionWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric StreamingExchangeSinksWithFullSendQueue;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int LOGICAL_ERROR;
    extern const int EXCHANGE_PEER_DISCONNECTED;
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
    if (connection_wait)
        ProfileEvents::increment(ProfileEvents::StreamingExchangeConnectionWaitMicroseconds, connection_wait->elapsedMicroseconds());
    connection_wait.reset();
    socket = std::make_unique<Poco::Net::StreamSocket>(future_connection->getSocket());
    future_connection.reset();
    chassert(socket);

    /// Set socket to non-blocking mode after handshake is finished.
    socket->setBlocking(false);
    socket->setSendBufferSize(1 * 1024 * 1024);

    /// Register the socket so the sink hears an inbound `NoMoreDataNeeded` while it waits.
    updateSocketWaitEvents();
}

void StreamingExchangeSink::updateSocketWaitEvents()
{
    uint32_t desired_events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
    if (hasUnsentBytes())
        desired_events |= EPOLLOUT;

    if (desired_events == registered_socket_events)
        return;

    /// `Epoll` has no modify operation; re-add the socket with the new event mask.
    if (registered_socket_events != 0)
    {
        wait_events_epoll.remove(socket->sockfd());
        registered_socket_events = 0;
    }
    wait_events_epoll.add(socket->sockfd(), desired_events);
    registered_socket_events = desired_events;
}

void StreamingExchangeSink::onUpdatePorts()
{
    /// Called by the executor on every port update while the sink is not idle, possibly from
    /// another thread. An extra wake is harmless: `work` drains it and `prepare` re-checks
    /// everything.
    port_update_wakeup.notify();
}

/// Send data to socket until the buffer is empty or until socket would block.
void StreamingExchangeSink::sendToSocket()
{
    /// Drain any inbound NoMoreDataNeeded / peer half-close before attempting to send.
    tryReceiveControlPacket();
    if (no_more_data_needed)
        return;

    while (!send_queue.empty())
    {
        try
        {
            /// `markNoMoreDataNeeded` clears `send_queue`, so we can't be in this loop.
            chassert(!no_more_data_needed);

            const std::string_view buffer = send_queue.front().bytes();
            size_t bytes_to_send = buffer.size() - send_position;
            /// Saturate at INT_MAX: a plain cast would wrap negative for buffers > 2 GiB, after
            /// which Poco's wrapper short-circuits without ever calling ::send.
            ssize_t sent = socket->sendBytes(
                buffer.data() + send_position,
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
                    StreamingExchangeProtocol::throwSocketError(last_error, *socket, "send data to exchange stream " + stream_name);
                }
            }

            LOG_TEST(log, "Sent {} bytes to exchange stream {}, fd: {}", sent, stream_name, socket->sockfd());

            send_position += sent;
            send_queue_bytes -= sent;
            total_bytes_sent += sent;
            ProfileEvents::increment(ProfileEvents::StreamingExchangeSendBytes, sent);
            if (send_position == buffer.size())
            {
                ProfileEvents::increment(ProfileEvents::StreamingExchangePacketsSent, send_queue.front().packets);
                send_queue.pop_front();
                send_position = 0;
            }
        }
        catch (const Poco::IOException & e)
        {
            /// Peer may have sent NoMoreDataNeeded or half-closed; only swallow the exception
            /// in those cases, otherwise it's a real network error.
            LOG_TRACE(log, "Send to exchange stream {} hit IO exception: {}; checking for peer close", stream_name, e.displayText());
            tryReceiveControlPacket();
            if (no_more_data_needed)
                return;
            StreamingExchangeProtocol::rethrowSocketException(*socket, "send data to exchange stream " + stream_name);
        }
    }
}

bool StreamingExchangeSink::canAddChunk() const
{
    return send_queue_bytes < MAX_PENDING_BYTES;
}

ISink::Status StreamingExchangeSink::waitForSendQueueRoom()
{
    if (!send_queue_full)
        send_queue_full.emplace(SendQueueFull{Stopwatch{}, CurrentMetrics::Increment{CurrentMetrics::StreamingExchangeSinksWithFullSendQueue}});
    return Status::Async;
}

void StreamingExchangeSink::enqueueBuffer(SendBuffer buffer)
{
    const size_t size = buffer.bytes().size();
    if (size == 0)
        return;

    send_queue_bytes += size;
    send_queue.push_back(std::move(buffer));
}

std::string_view StreamingExchangeSink::SendBuffer::bytes() const
{
    if (const auto * column = std::get_if<ColumnPtr>(&data))
        return (*column)->getDataAt(0);
    return std::get<String>(data);
}

ISink::Status StreamingExchangeSink::prepare()
{
    /// If socket is not ready yet, wait for it
    if (!socket)
    {
        if (!connection_wait)
            connection_wait.emplace();
        return Status::Async;
    }

    if (send_queue_full && canAddChunk())
    {
        ProfileEvents::increment(ProfileEvents::StreamingExchangeSendQueueFullMicroseconds, send_queue_full->since.elapsedMicroseconds());
        send_queue_full.reset();
    }

    /// The peer will not read this stream anymore (for example, its LIMIT is satisfied).
    /// Close the input so the stop propagates to the upstream stages; without this they
    /// would keep computing data that nobody reads.
    if (no_more_data_needed)
    {
        LOG_TRACE(log, "Closing input of exchange stream {}, no more data needed", stream_name);
        input.close();
        return Status::Finished;
    }

    if (has_input)
        return canAddChunk() ? Status::Ready : waitForSendQueueRoom();

    if (input.isFinished())
    {
        if (!end_of_stream_added)
        {
            if (!canAddChunk())
                return waitForSendQueueRoom();
            /// The input is finished: queue the end-of-stream packet.
            input_is_finished = true;
            current_chunk = {};
            has_input = true;
            return Status::Ready;
        }

        /// Send what is still queued.
        if (hasUnsentBytes())
            return Status::Async;

        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    /// Propagate back-pressure upstream: don't pull until there's room.
    if (!canAddChunk())
        return waitForSendQueueRoom();

    input.setNeeded();
    if (!input.hasData())
    {
        /// Wait on the socket, not only on the input port: an inbound `NoMoreDataNeeded`
        /// must wake the sink even if this stage never produces another chunk. Pending packets
        /// go out on the same wait; without that they would sit here until the next chunk
        /// arrives, and that can take a long time. `onUpdatePorts` wakes the sink when input arrives.
        return Status::Async;
    }

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

    /// Drain the wakeup pipe; otherwise it would stay readable and wake the sink again at once.
    port_update_wakeup.drain();

    /// React to EPOLLIN / EPOLLRDHUP wakeups (see scheduleForEvent).
    tryReceiveControlPacket();

    if (has_input)
    {
        /// Nothing follows the end-of-stream packet.
        chassert(!end_of_stream_added);

        has_input = false;
        if (input_is_finished)
        {
            chassert(!current_chunk);
            end_of_stream_added = true;
            sendEndOfStream();
        }
        else if (current_chunk)
        {
            /// If the chunk is not the final one, send it only if it is not empty
            consume(std::move(current_chunk));
        }

        return;
    }

    if (hasUnsentBytes())
    {
        sendToSocket();
        return;
    }

    /// Without the `end_of_stream_added` check, a wake with an empty input and empty buffers
    /// (for example the port-update wakeup) would call `onFinish` while the stream is still
    /// open.
    if (end_of_stream_added && !was_on_finish_called)
    {
        was_on_finish_called = true;
        onFinish();
        return;
    }
}

std::tuple<int, uint32_t, int64_t> StreamingExchangeSink::scheduleForEvent()
{
    if (socket)
    {
        updateSocketWaitEvents();
        LOG_TEST(log, "Schedule exchange stream sink {}, socket is ready, fd: {}", stream_name, socket->sockfd());
        /// `wait_events_epoll` becomes readable on socket events (inbound `NoMoreDataNeeded`,
        /// peer close, writability while there are unsent bytes) and on the port-update wakeup.
        /// No timeout: socket events and port updates each wake the sink explicitly; a timeout
        /// would only hide a missed wakeup as a delay instead of a visible hang.
        return {wait_events_epoll.getFileDescriptor(), EPOLLIN | EPOLLERR, -1};
    }

    if (!future_connection)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Future connection is not set for exchange stream {}", stream_name);

    /// Wait on the eventfd; `work` extracts the socket after the wake. The eventfd stays
    /// readable once the connection is ready, so the wake is immediate even if the connection
    /// got ready before this call. Extracting the socket here instead would skip that wake and
    /// `prepare` would never run with the socket: the sink would sleep on a quiet socket
    /// without ever marking its input as needed, and the fragment would never start.
    int fd = future_connection->getEventFd();

    LOG_TEST(log, "Schedule exchange stream sink {} waiting for connection, eventfd: {}", stream_name, fd);
    return {fd, EPOLLIN | EPOLLERR, -1};
}

void StreamingExchangeSink::sendEndOfStream()
{
    if (no_more_data_needed)
        return;

    LOG_TEST(log, "Writing the end-of-stream packet to exchange stream {}", stream_name);
    WriteBufferFromOwnString packet;
    StreamingExchangeProtocol::writeEndOfStreamPacket(packet);
    packet.finalize();
    enqueueBuffer(SendBuffer{.data = std::move(packet.str()), .packets = 1});
    sendToSocket();
}

void StreamingExchangeSink::consume(Chunk chunk)
{
    if (no_more_data_needed)
    {
        /// `prepare` stops the sink when it sees `no_more_data_needed`, but the sink can pull
        /// a chunk before the packet arrives - drop it.
        LOG_TEST(log, "No more data needed for exchange stream {}, dropping chunk with {} rows", stream_name, chunk.getNumRows());
        return;
    }

    ++chunks_written;

    /// A packet is sent from its own column, which the sinks of the other destinations of a
    /// broadcast share.
    if (chunk.getNumRows() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Exchange stream {} expects one packet per chunk, got a chunk with {} rows", stream_name, chunk.getNumRows());

    LOG_TEST(log, "Writing a packet of {} bytes to exchange stream {}", chunk.getColumns().front()->getDataAt(0).size(), stream_name);
    enqueueBuffer(SendBuffer{.data = chunk.getColumns().front(), .packets = 1});
    sendToSocket();
}

void StreamingExchangeSink::onFinish()
{
    LOG_TRACE(log, "Finished writing to exchange stream {}, chunks: {}, bytes: {}",
        stream_name, chunks_written, total_bytes_sent);
}

bool StreamingExchangeSink::tryReadFromSocketNonBlocking(char * buffer, size_t buffer_size, size_t & position)
{
    while (position < buffer_size)
    {
        ssize_t received = StreamingExchangeProtocol::tryReceive(
            *socket, buffer + position, buffer_size - position, "control packet on exchange stream " + stream_name);
        if (received < 0)
            return false; /// Peer half-closed.
        if (received == 0)
            return true; /// No data right now, try later.
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
        /// The consumer went away mid-stream, most likely because its task failed or was cancelled.
        if (incoming_packet_bytes_filled > 0)
            throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED,
                "Peer half-closed exchange stream {} after {} of {} control bytes; truncated control message",
                stream_name, incoming_packet_bytes_filled, sizeof(incoming_packet_type));

        /// Normal end of the stream: after the sink sent the end-of-stream packet, the source reads it
        /// and closes without sending NoMoreDataNeeded. EOF is fine only with nothing left to send.
        if (!end_of_stream_added || send_queue_bytes > 0)
            throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED,
                "Peer half-closed exchange stream {} without sending NoMoreDataNeeded "
                "(end_of_stream_added={}, unsent={})",
                stream_name, end_of_stream_added, send_queue_bytes);

        LOG_TRACE(log, "Peer closed exchange stream {} after reading the end-of-stream packet", stream_name);
        markNoMoreDataNeeded();
    }
}

void StreamingExchangeSink::markNoMoreDataNeeded()
{
    no_more_data_needed = true;
    /// Drop pending output: consume() drops new chunks too, no more sends will happen.
    send_queue.clear();
    send_queue_bytes = 0;
    send_position = 0;
}

}

#endif
