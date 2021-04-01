#include <IO/MySQLPacketPayloadReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb

MySQLPacketPayloadReadBuffer::MySQLPacketPayloadReadBuffer(ReadBuffer & in_, uint8_t & sequence_id_)
    : ReadBuffer(in_.position(), 0), in(in_), sequence_id(sequence_id_) // not in.buffer().begin(), because working buffer may include previous packet
{
}

bool MySQLPacketPayloadReadBuffer::nextImpl()
{
    if (!has_read_header || (payload_length == MAX_PACKET_LENGTH && offset == payload_length))
    {
        has_read_header = true;
        working_buffer.resize(0);
        offset = 0;
        payload_length = 0;
        in.readStrict(reinterpret_cast<char *>(&payload_length), 3);

        if (payload_length > MAX_PACKET_LENGTH)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Received packet with payload larger than max_packet_size: {}", payload_length);

        size_t packet_sequence_id = 0;
        in.read(reinterpret_cast<char &>(packet_sequence_id));
        if (packet_sequence_id != sequence_id)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Received packet with wrong sequence-id: {}. Expected: {}.", packet_sequence_id, static_cast<unsigned int>(sequence_id));
        sequence_id++;

        if (payload_length == 0)
            return false;
    }
    else if (offset == payload_length)
    {
        return false;
    }

    in.nextIfAtEnd();
    working_buffer = ReadBuffer::Buffer(in.position(), in.buffer().end());
    size_t count = std::min(in.available(), payload_length - offset);
    working_buffer.resize(count);
    in.ignore(count);

    offset += count;

    return true;
}

}
