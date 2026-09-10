#include <Server/DistributedQuery/StreamingExchangeProtocol.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/StreamSocket.h>

#include <algorithm>
#include <climits>
#include <cerrno>
#include <cstddef>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCHANGE_PEER_DISCONNECTED;
    extern const int NOT_IMPLEMENTED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

namespace StreamingExchangeProtocol
{

UInt64 SourceHelloBody::readVersion(ReadBuffer & in)
{
    UInt64 version = 0;
    readIntBinary(version, in);
    return version;
}

void SourceHelloBody::readAfterVersion(ReadBuffer & in)
{
    readStringBinary(query_id, in);
    readStringBinary(stream_name, in);
    readStringBinary(auth_token, in);
}

void SourceHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(source_version, out);
    writeStringBinary(query_id, out);
    writeStringBinary(stream_name, out);
    writeStringBinary(auth_token, out);
}

void SinkHelloBody::read(ReadBuffer & in)
{
    readIntBinary(sink_version, in);
}

void SinkHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(sink_version, out);
}

namespace
{
    /// The errors `recv` and `send` report when the other side of an established connection is gone.
    bool isPeerGoneError(int socket_errno)
    {
        return socket_errno == ECONNRESET || socket_errno == ECONNABORTED || socket_errno == EPIPE
            || socket_errno == ENETRESET || socket_errno == ENOTCONN || socket_errno == ETIMEDOUT;
    }
}

size_t writeDataPacket(const Chunk & chunk, const SharedHeader & header, WriteBuffer & out)
{
    const size_t packet_offset = out.count();
    PacketHeader packet_header{.packet_type = PacketType::Data, .bytes_size = 0};
    out.write(reinterpret_cast<const char *>(&packet_header), sizeof(packet_header));

    const bool final_chunk = chunk.empty();
    auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
    UInt64 flags = 0;
    if (final_chunk)
        flags |= 1;
    if (agg_info)
        flags |= 2;
    writeVarUInt(flags, out);
    writeVarUInt(chunk.getNumRows(), out);
    writeVarUInt(chunk.getNumColumns(), out);
    /// chunk_num has no BlockInfo field; carry it in the exchange framing so memory-bound merging
    /// can restore chunk order on the receiver.
    if (agg_info)
        writeVarUInt(agg_info->chunk_num, out);

    if (chunk.getNumColumns() > 0)
    {
        /// The exchange stream uses the server default codec: `network_compression_method` is a
        /// per-query setting and the sender has no query settings at hand. This is safe: each
        /// compressed frame is self-describing (the receiver auto-detects the codec via
        /// `CompressedReadBuffer`), and the exchange is a transient, same-version channel -
        /// the handshake rejects peers on a different protocol version, so a stream is never read
        /// back by a node expecting a different codec.
        CompressedWriteBuffer compressed_buf(out);
        try
        {
            NativeWriter writer(compressed_buf, DBMS_TCP_PROTOCOL_VERSION, header);
            Block block = header->cloneWithColumns(chunk.getColumns());
            /// Carry the remaining aggregation metadata in block.info, the same way partial-aggregation
            /// results are transported for distributed/parallel reads.
            if (agg_info)
            {
                block.info.bucket_num = agg_info->bucket_num;
                block.info.is_overflows = agg_info->is_overflows;
                block.info.out_of_order_buckets = agg_info->out_of_order_buckets;
            }
            writer.write(block);
            writer.flush();
            compressed_buf.finalize();
        }
        catch (...)
        {
            compressed_buf.cancel();
            throw;
        }
    }

    return packet_offset;
}

void finishDataPacket(char * packet, size_t packet_bytes)
{
    const size_t packet_data_size = packet_bytes - sizeof(PacketHeader);

    /// The receiver rejects Data packets above this limit; fail here with a clear, local error
    /// instead of sending one the peer would reject. Splitting large chunks is not implemented yet.
    if (packet_data_size > MAX_DATA_PACKET_BODY_BYTES)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Exchange data packet of {} bytes exceeds the maximum {}; splitting large chunks is not implemented",
            packet_data_size, MAX_DATA_PACKET_BODY_BYTES);

    /// memcpy: the header may sit at an unaligned offset of the buffer.
    static_assert(sizeof(PacketHeader::bytes_size) == sizeof(packet_data_size));
    memcpy(packet + offsetof(PacketHeader, bytes_size), &packet_data_size, sizeof(packet_data_size));
}

const SharedHeader & packetStreamHeader()
{
    static const SharedHeader header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "__streaming_exchange_packet")});
    return header;
}

DataPacketPrefix readDataPacketPrefix(const char * body, size_t body_size, const String & stream_name)
{
    ReadBufferFromMemory in(body, body_size);
    UInt64 flags = 0;
    readVarUInt(flags, in);
    DataPacketPrefix prefix;
    prefix.end_of_stream = flags & 1;
    readVarUInt(prefix.num_rows, in);
    UInt64 num_columns = 0;
    readVarUInt(num_columns, in);
    if (flags & 2)
    {
        UInt64 chunk_num = 0;
        readVarUInt(chunk_num, in);
    }

    if (prefix.end_of_stream && (prefix.num_rows != 0 || num_columns != 0 || !in.eof()))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Final data packet on exchange stream {} is not the empty end-of-stream marker: {} rows, {} columns, {} bytes after the fields",
            stream_name, prefix.num_rows, num_columns, in.available());
    return prefix;
}

DataPacket readDataPacketBody(ReadBuffer & body, const Block & header, const String & stream_name)
{
    UInt64 flags = 0;
    readVarUInt(flags, body);
    const bool end_of_stream = flags & 1;
    const bool has_aggregated_chunk_info = flags & 2;
    UInt64 num_rows = 0;
    readVarUInt(num_rows, body);
    UInt64 num_columns = 0;
    readVarUInt(num_columns, body);
    UInt64 chunk_num = 0;
    if (has_aggregated_chunk_info)
        readVarUInt(chunk_num, body);

    /// The end-of-stream packet is empty. One carrying rows or columns would have them dropped once
    /// the stream is finished, so reject it as a protocol violation.
    if (end_of_stream && (num_rows != 0 || num_columns != 0))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Final data packet on exchange stream {} is not the empty end-of-stream marker: {} rows, {} columns",
            stream_name, num_rows, num_columns);

    /// A data packet must carry exactly the header's columns, or values would be dropped while the
    /// row count is kept. A header-less stream (e.g. SELECT count()) sends rows with zero columns.
    if (num_rows != 0 && num_columns != header.columns())
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Data packet on exchange stream {} carries {} rows with {} columns, but the stream header has {} columns",
            stream_name, num_rows, num_columns, header.columns());

    DataPacket packet;
    packet.end_of_stream = end_of_stream;
    if (num_columns != 0)
    {
        CompressedReadBuffer compressed_buf(body);
        NativeReader reader(compressed_buf, header, DBMS_TCP_PROTOCOL_VERSION);
        Block block = reader.read();
        packet.chunk = Chunk(block.getColumns(), num_rows);
        if (has_aggregated_chunk_info)
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            info->out_of_order_buckets = block.info.out_of_order_buckets;
            info->chunk_num = chunk_num;
            packet.chunk.getChunkInfos().add(std::move(info));
        }
    }
    else if (num_rows == 0)
        packet.chunk = Chunk(header.cloneEmptyColumns(), 0);
    else
        packet.chunk = Chunk(Columns{}, num_rows);
    return packet;
}

String describePeer(const Poco::Net::StreamSocket & socket)
{
    try
    {
        return socket.peerAddress().toString();
    }
    catch (const Poco::Exception &)
    {
        return "unknown peer";
    }
}

void throwSocketError(int socket_errno, const Poco::Net::StreamSocket & socket, const String & what)
{
    if (isPeerGoneError(socket_errno))
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}), errno {}", what, describePeer(socket), socket_errno);
    throw Poco::Net::NetException(fmt::format("Failed to {} ({}), errno {}", what, describePeer(socket), socket_errno));
}

void rethrowSocketException(const Poco::Net::StreamSocket & socket, const String & what)
{
    try
    {
        throw;
    }
    catch (const Poco::IOException & e)
    {
        if (!isPeerGoneError(e.code()))
            throw;
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}): {}", what, describePeer(socket), e.displayText());
    }
    catch (const Poco::TimeoutException & e)
    {
        /// The kernel's connection timeout (`ETIMEDOUT`: the peer stopped answering) comes as a timeout
        /// exception carrying that errno. A deadline that ran out carries `EAGAIN` and stays a timeout.
        if (e.code() != ETIMEDOUT)
            throw;
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}), connection timed out", what, describePeer(socket));
    }
}

ssize_t tryReceive(Poco::Net::StreamSocket & socket, char * buffer, size_t size, const String & description)
{
    /// Poco's receiveBytes takes int. Cap the request at INT_MAX so a >2 GiB buffer
    /// (the data-path body is sized by an untrusted peer) does not wrap negative.
    const int chunk = static_cast<int>(std::min<size_t>(size, INT_MAX));
    while (true)
    {
        ssize_t received = 0;
        try
        {
            received = socket.receiveBytes(buffer, chunk);
        }
        catch (const Poco::Exception &)
        {
            rethrowSocketException(socket, "receive " + description);
        }
        if (received > 0)
            return received;
        if (received == 0)
            return -1;

        const int last_error = errno;
        if (last_error == EINTR)
            continue;
        if (last_error == EAGAIN || last_error == EWOULDBLOCK)
            return 0;
        throwSocketError(last_error, socket, "receive " + description);
    }
}

void sendAll(Poco::Net::StreamSocket & socket, const char * buffer, size_t size, const String & description)
{
    size_t position = 0;
    while (position < size)
    {
        const int chunk = static_cast<int>(std::min<size_t>(size - position, INT_MAX));
        int sent = 0;
        try
        {
            sent = socket.sendBytes(buffer + position, chunk);
        }
        catch (const Poco::Exception &)
        {
            rethrowSocketException(socket, "send " + description);
        }
        if (sent >= 0)
        {
            position += sent;
            continue;
        }

        const int last_error = errno;
        if (last_error == EINTR)
            continue;
        throwSocketError(last_error, socket, "send " + description);
    }
}

}
}
