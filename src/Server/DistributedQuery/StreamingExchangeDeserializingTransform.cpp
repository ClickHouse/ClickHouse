#include <Server/DistributedQuery/StreamingExchangeDeserializingTransform.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

StreamingExchangeDeserializingTransform::StreamingExchangeDeserializingTransform(SharedHeader output_header, String exchange_id_)
    : ISimpleTransform(StreamingExchangeProtocol::packetStreamHeader(), std::move(output_header), /*skip_empty_chunks_=*/ false)
    , exchange_id(std::move(exchange_id_))
{
}

void StreamingExchangeDeserializingTransform::transform(Chunk & chunk)
{
    if (chunk.getNumRows() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The deserializer of exchange {} expects one packet per chunk, got {} rows", exchange_id, chunk.getNumRows());

    const std::string_view packet = chunk.getColumns().front()->getDataAt(0);
    StreamingExchangeProtocol::PacketHeader packet_header{};
    if (packet.size() < sizeof(packet_header))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Packet of {} bytes on exchange {} is shorter than a packet header", packet.size(), exchange_id);
    memcpy(&packet_header, packet.data(), sizeof(packet_header));
    if (packet_header.packet_type != StreamingExchangeProtocol::PacketType::Data
        || packet_header.bytes_size != packet.size() - sizeof(packet_header))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Malformed packet on exchange {}: type {}, body size {} in {} bytes",
            exchange_id, packet_header.packet_type, packet_header.bytes_size, packet.size());

    ReadBufferFromMemory body(packet.data() + sizeof(packet_header), packet_header.bytes_size);
    auto data_packet = StreamingExchangeProtocol::readDataPacketBody(body, getOutputPort().getHeader(), exchange_id);
    /// The source reads the end-of-stream marker itself and does not hand it on.
    if (data_packet.end_of_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "End-of-stream packet reached the deserializer of exchange {}", exchange_id);

    chunk = std::move(data_packet.chunk);
}

}
