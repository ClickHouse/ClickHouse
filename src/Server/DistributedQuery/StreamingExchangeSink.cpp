#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/logger_useful.h>
#include <Poco/Net/NetException.h>
#include <sys/epoll.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

StreamingExchangeSink::~StreamingExchangeSink()
{
    if (out && !out->isFinalized())
        out->cancel();
}

void StreamingExchangeSink::onStart()
{
    connect();
    sendHello();
    receiveHello();

    /// Set socket to non-blocking mode after handshake is finished.
    socket->setBlocking(false);
    /// Prepare initial in-memory buffer for serializing chunks
    out = std::make_shared<WriteBufferFromOwnString>();
}

/// Send data to socket until the buffer is empty or until socket would block.
void StreamingExchangeSink::sendToSocket()
{
    while (current_send_position_in_buffer < current_send_buffer.size())
    {
        size_t bytes_to_send = current_send_buffer.size() - current_send_position_in_buffer;
        ssize_t sent = socket->sendBytes(current_send_buffer.data() + current_send_position_in_buffer, bytes_to_send);
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
                throw Poco::Net::NetException("Failed to send data to socket", last_error);
            }
        }

        LOG_TEST(log, "Sent {} bytes to exchange stream {}, fd: {}", sent, stream_name, socket->sockfd());

        current_send_position_in_buffer += sent;
        total_bytes_sent += sent;
    }

    /// Is there enough serialized data to start sending it to socket?
    if (out->count() >= FLUSH_BUFFER_TO_SOCKET_THRESHOLD)
        tryToSwitchSendBuffer();
}

bool StreamingExchangeSink::canAddChunk() const
{
    return out->count() < 2 * FLUSH_BUFFER_TO_SOCKET_THRESHOLD;
}

void StreamingExchangeSink::tryToSwitchSendBuffer()
{
    /// Check that current_send_buffer has been fully sent to socket
    if (current_send_position_in_buffer < current_send_buffer.size())
        return;

    out->finalize();
    current_send_buffer = out->str();
    current_send_position_in_buffer = 0;
    out = std::make_shared<WriteBufferFromOwnString>();
}

ISink::Status StreamingExchangeSink::prepare()
{
    if (!was_on_start_called)
        return Status::Ready;

    if (has_input)
        return canAddChunk() ? Status::Ready : Status::Async;

    if (input.isFinished())
    {
        if (!final_chunk_added)
        {
            /// Input is finished, so we need to send one empty chunk to signal that we are done.
            input_is_finished = true;
            current_chunk = {};
            has_input = true;
            return canAddChunk() ? Status::Ready : Status::Async;
        }

        /// Need ot flush all remaining data
        if (current_send_position_in_buffer < current_send_buffer.size() || out->count() > 0)
            return Status::Async;

        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return canAddChunk() ? Status::Ready : Status::Async;
}

void StreamingExchangeSink::work()
{
    if (!was_on_start_called)
    {
        was_on_start_called = true;
        onStart();
        return;
    }

    if (has_input)
    {
        /// If we have already added final chunk then no new input is expected
        assert(!final_chunk_added);

        has_input = false;
        if (input_is_finished)
        {
            assert(!current_chunk);
            final_chunk_added = true;
        }
        consume(std::move(current_chunk));
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
    LOG_TEST(log, "Schedule exchange stream sink {}, fd: {}", stream_name, socket->sockfd());

    return {socket->sockfd(), EPOLL_EVENTS::EPOLLOUT | EPOLL_EVENTS::EPOLLERR};
}

void StreamingExchangeSink::consume(Chunk chunk)
{
    rows_written += chunk.getNumRows();

    if (chunk.getNumRows() == 0 && chunk.getNumColumns() != 0)
    {
        LOG_TEST(log, "Unexpected chunk with 0 rows to exchange stream {}", stream_name);
        return;
    }

    LOG_TEST(log, "Writing chunk with {} rows to exchange stream {}", chunk.getNumRows(), stream_name);

    /// Write packet type
    writeVarUInt(StreamingExchangeProtocol::PacketType::Data, *out);

    writeVarUInt(chunk.getNumRows(), *out);

    if (chunk.getNumRows() > 0)
    {
        auto compressed_buf = std::make_unique<CompressedWriteBuffer>(*out);
        auto writer = std::make_unique<NativeWriter>(*compressed_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS, input.getHeader());

        Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
        writer->write(block);

        writer->flush();
        compressed_buf->finalize();
    }

    sendToSocket();
}

void StreamingExchangeSink::onFinish()
{
    LOG_TRACE(log, "Finished writing to exchange stream {}, total rows: {}, bytes: {}",
        stream_name, rows_written, total_bytes_sent);
}

void StreamingExchangeSink::connect()
{
    socket = std::make_unique<Poco::Net::StreamSocket>();
    Poco::Net::SocketAddress address(host, port);
    socket->connect(address);
    socket->setSendBufferSize(1 * 1024 * 1024);
    in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
}

void StreamingExchangeSink::sendHello()
{
    WriteBufferFromPocoSocket hello_out(*socket);
    writeVarUInt(StreamingExchangeProtocol::PacketType::SinkHello, hello_out);
    writeStringBinary(query_id, hello_out);
    writeStringBinary(stream_name, hello_out);
    hello_out.next();
    hello_out.cancel();
}

void StreamingExchangeSink::receiveHello()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != StreamingExchangeProtocol::PacketType::SourceHello)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);
}

}
