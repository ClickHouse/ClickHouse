#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteHelpers.h>

/// TODO: use separate protocol for streaming exchange
#include <Core/Protocol.h>
#include "IO/VarInt.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

void StreamingExchangeSink::onStart()
{
    connect();
    sendHello();
    receiveHello();
}

void StreamingExchangeSink::consume(Chunk chunk)
{
    rows_written += chunk.getNumRows();

    LOG_TEST(log, "Writing chunk with {} rows to exchange stream {}", chunk.getNumRows(), stream_name);

    /// TODO: write packet type
    writeVarUInt(Protocol::Client::Data, *out); /// TODO: use separate protocol for streaming exchange

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

    /// TODO: what is the proper way to finish a packet?
    out->finishChunk();
    out->next();

    /// TODO: finalize out buffer when last (empty) chunk is consumed?
}

void StreamingExchangeSink::onFinish()
{
    /// Send empty chunk to indicate end of data
    /// TODO: is it correct?
    consume({});

    out->finalize();
}

void StreamingExchangeSink::connect()
{
    socket = std::make_unique<Poco::Net::StreamSocket>();
    Poco::Net::SocketAddress address(host, port);
    socket->connect(address);
    in = std::make_shared<ReadBufferFromPocoSocketChunked>(*socket);
    out = std::make_shared<WriteBufferFromPocoSocketChunked>(*socket);
}

void StreamingExchangeSink::sendHello()
{
    writeVarUInt(Protocol::Client::Hello, *out); /// TODO: use separate protocol for streaming exchange
    writeStringBinary(query_id, *out);
    writeStringBinary(stream_name, *out);
    out->next();
}

void StreamingExchangeSink::receiveHello()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in); /// TODO: use separate protocol for streaming exchange
    if (packet_type != Protocol::Server::Hello)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);
}

}
