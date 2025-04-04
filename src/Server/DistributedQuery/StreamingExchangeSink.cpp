#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>
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
}

ISink::Status StreamingExchangeSink::prepare()
{
    if (!was_on_start_called)
        return Status::Ready;

    if (has_input)
        return wait_for_out_socket_ready ? Status::Async : Status::Ready;

    if (input.isFinished())
    {
        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return wait_for_out_socket_ready ? Status::Async : Status::Ready;
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

    out->finishChunk();

    /// Check if we buffered enough data to send to socket
    if (out->count() > bytes_sent_to_socket + bytes_sent_to_socket_threshold)
    {
        out->next();
        bytes_sent_to_socket = out->count();
        wait_for_out_socket_ready = true;
    }
}

void StreamingExchangeSink::onFinish()
{
    /// Send empty chunk to indicate end of data
    consume({});

    out->finalize();

    size_t total_bytes_sent = out->count();

    LOG_TEST(log, "Finished writing to exchange stream {}, total rows: {}, bytes: {}",
        stream_name, rows_written, total_bytes_sent);
}

void StreamingExchangeSink::connect()
{
    socket = std::make_unique<Poco::Net::StreamSocket>();
    Poco::Net::SocketAddress address(host, port);
    socket->connect(address);
    socket->setSendBufferSize(1 * 1024 * 1024);
    in = std::make_shared<ReadBufferFromPocoSocketChunked>(*socket);
    out = std::make_shared<WriteBufferFromPocoSocketChunked>(*socket);
}

void StreamingExchangeSink::sendHello()
{
    writeVarUInt(StreamingExchangeProtocol::PacketType::SinkHello, *out);
    writeStringBinary(query_id, *out);
    writeStringBinary(stream_name, *out);
    out->next();
}

void StreamingExchangeSink::receiveHello()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != StreamingExchangeProtocol::PacketType::SourceHello)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);
}

}
